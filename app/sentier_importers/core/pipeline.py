"""Pipeline driver: run a source through
fetch → parse → transform → dedup → assemble → validate → emit → deliver."""

from dataclasses import replace
from pathlib import Path

from sentier_importers.core import dedup as dedup_mod
from sentier_importers.core import deliver as deliver_mod
from sentier_importers.core import inventory_lineage
from sentier_importers.core import lineage as lineage_mod
from sentier_importers.core import schema_provider
from sentier_importers.core import validate as validate_mod
from sentier_importers.core import write as write_mod
from sentier_importers.core.context import RunContext
from sentier_importers.core.errors import FetchError, SourceContractError, ValidationError
from sentier_importers.core.registry import DATA_ROOT_VAR
from sentier_importers.core.source import Source, SourceConfig
from sentier_importers.core.targets import Target, get_target
from sentier_importers.core.types import Payload, Rows


def _assemble(rows: Rows, config: SourceConfig) -> Payload:
    """Wrap rows into a ``{scheme, <items_key>: [...]}`` collection, or pass through.

    Returns ``rows`` unchanged for bulk/non-collection targets (``collection_class``
    unset); a randonneur mapping package when ``package_verb`` is set; otherwise the
    assembled collection mapping matching ``data/<category>/*``.
    """
    if config.package_verb is not None:
        return {
            "name": config.package_name,
            "version": config.package_version,
            config.package_verb: rows,
        }
    if config.collection_class is None:
        return rows
    return {"scheme": config.collection_scheme, config.collection_items_key: rows}


def _validate(payload: Payload, config: SourceConfig, target: Target, ctx: RunContext) -> None:
    """Validate the assembled collection (tree-root) or each row (legacy per-item)."""
    if config.collection_class is not None:
        if config.schema_file is None:
            raise ValidationError(
                f"source {config.name!r} sets a collection but no schema_file to resolve"
            )
        schema_path = schema_provider.resolve_schema(target, config.schema_file, ctx)
        validate_mod.validate(payload, "linkml_collection", schema_path, config.collection_class)
    elif config.validate_against is not None:
        schema_path = schema_provider.resolve_schema(target, config.validate_against, ctx)
        validate_mod.validate(payload, target.validator, schema_path, config.validate_against)


def _arrow_schema(config: SourceConfig, target: Target, ctx: RunContext):
    """Explicit Arrow schema for a Parquet collection target, else None.

    Built from the item class (``validate_against``) in the resolved LinkML schema, so
    optional columns are never dropped. The schema fetch is a cache hit (already pulled
    during validation). Non-parquet or non-collection targets need no schema.
    """
    if config.output_format != "parquet" or config.collection_class is None:
        return None
    if config.schema_file is None or config.validate_against is None:
        return None
    schema_path = schema_provider.resolve_schema(target, config.schema_file, ctx)
    return write_mod.arrow_schema_for(schema_path, config.validate_against)


#: What to do about a missing local input, by the source's ``access`` class.
ACCESS_HINTS = {
    "public": "This input is a public sentier-* checkout: clone it beside this repo "
    "or point the data root at it.",
    "licensed": "This input is obtainable from its provider under the provider's "
    "terms; place it at the path above.",
    "private": "This input is a DdS-internal artifact and is not distributed with " "the repo.",
}


def _input_name(config: SourceConfig, url: str | None) -> str:
    """The registry key (``fetch.url`` or an ``inputs`` name) that declares ``url``."""
    if url == config.fetch_url:
        return "fetch.url"
    for name, candidate in config.inputs.items():
        if candidate == url:
            return name
    return "an input"


def _with_input_context(exc: FetchError, config: SourceConfig, ctx: RunContext) -> FetchError:
    """Re-wrap a fetch failure with the source, the registry input, the data root
    that was used, and what the ``access`` class says about obtaining the file.
    The exception type is preserved so the CLI can tell a missing local input
    (warn and skip) from any other fetch failure (error)."""
    root = ctx.data_root if ctx.data_root is not None else "(not set)"
    lines = [
        str(exc),
        f"  input:  {_input_name(config, exc.url)} of source {config.name!r}",
        f"  root:   {root}  (override with --data-root or {DATA_ROOT_VAR})",
        f"  access: {config.access}. {ACCESS_HINTS[config.access]}",
    ]
    return type(exc)("\n".join(lines), url=exc.url)


def _fetch(source: Source, ctx: RunContext):
    try:
        return source.fetch(ctx)
    except FetchError as exc:
        raise _with_input_context(exc, source.config, ctx) from exc


def _produce(
    source: Source, ctx: RunContext, recorder: lineage_mod.LineageRecorder
) -> tuple[Rows, Payload, Target, lineage_mod.DatasetRef]:
    """Run fetch → parse → transform → dedup → assemble → validate.

    Returns deduped rows, emit-ready payload, target, and validated dataset identity.
    """
    config = source.config
    target = get_target(config.target)
    external = recorder._inputs

    def fetch():
        raw = _fetch(source, ctx)
        if isinstance(raw, lineage_mod.RawData):
            external.append(
                inventory_lineage.observe_dependency(recorder, "source", config.fetch_url, raw)
            )
        return raw

    def observed(stage, allowed=None):
        def run(operation, inputs):
            direct_reads = []

            def consume(name, value):
                if name not in config.inputs or (allowed is not None and name not in allowed):
                    raise SourceContractError(f"step {stage!r} used undeclared input {name!r}")
                ref = inventory_lineage.observe_dependency(
                    recorder, name, config.inputs[name], value
                )
                if isinstance(value, Path):
                    direct_reads.append((value, ref.version))
                if not any(
                    item.namespace == ref.namespace and item.name == ref.name for item in inputs
                ):
                    inputs.append(ref)
                if not any(
                    item.namespace == ref.namespace and item.name == ref.name
                    for item in recorder._inputs
                ):
                    recorder._inputs.append(ref)

            source._input_observer = consume
            try:
                result = operation()
                for path, expected in direct_reads:
                    if "sha256:" + inventory_lineage.path_fingerprint(path)[0] != expected:
                        raise SourceContractError(
                            "Direct input changed while the step was running"
                        )
                return result
            finally:
                source._input_observer = None

        return run

    fetched = recorder.intermediate("fetched-source")
    raw = recorder.run_child(
        "fetch",
        "Fetch and cache the primary source and its named inputs.",
        external,
        [fetched],
        fetch,
        source=source.fetch,
    )

    parsed = recorder.intermediate("parsed-records")
    parse_inputs = [fetched]
    records = recorder.run_child(
        "parse",
        f"Parse the fetched {config.fetch_format} source into records.",
        parse_inputs,
        [parsed],
        lambda: observed("parse")(lambda: source.parse(raw), parse_inputs),
        source=source.parse,
    )

    current = parsed
    transformed = records
    for step in source.declared_transform_steps():
        output = recorder.intermediate(step.name)
        step_inputs = [current]
        transformed = recorder.run_child(
            step.name,
            step.description,
            step_inputs,
            [output],
            lambda step=step, transformed=transformed: observed(step.name, step.inputs)(
                lambda: step.function(transformed), step_inputs
            ),
            source=step.function,
            code_dependencies=step.code_dependencies,
            include_source=True,
        )
        current = output

    deduped = recorder.intermediate("deduplicated-rows")
    dedup_inputs = [current]

    def framework_input(role, url, raw):
        ref = inventory_lineage.observe_dependency(recorder, role, url, raw)
        dedup_inputs.append(ref)
        recorder._inputs.append(ref)

    dedup_ctx = replace(ctx, dependency_observer=framework_input)
    rows = recorder.run_child(
        "deduplicate",
        "Deduplicate transformed rows according to the source registry policy.",
        dedup_inputs,
        [deduped],
        lambda: dedup_mod.dedup(transformed, config, target, dedup_ctx),
        source=dedup_mod.dedup,
    )

    assembled = recorder.intermediate("assembled-payload")
    payload = recorder.run_child(
        "assemble",
        "Assemble rows into the payload shape required by the target.",
        [deduped],
        [assembled],
        lambda: _assemble(rows, config),
        source=_assemble,
    )

    validated = recorder.intermediate("validated-payload")

    def validate() -> Payload:
        _validate(payload, config, target, ctx)
        return payload

    recorder.run_child(
        "validate",
        "Validate the assembled payload against the configured target contract.",
        [assembled],
        [validated],
        validate,
        source=_validate,
    )
    return rows, payload, target, validated


def validate_source(source: Source, ctx: RunContext) -> int:
    """Run the pipeline up to and including validation; return the row count."""
    config = source.config
    recorder = lineage_mod.LineageRecorder(
        config, lineage_mod.lineage_path(config, ctx), parent_source=type(source), ctx=ctx
    )
    recorder.start_parent([])
    try:
        rows, _, _, validated = _produce(source, ctx, recorder)
        recorder.complete_parent([validated])
        return len(rows)
    except BaseException as error:
        recorder.fail_parent(error)
        raise
    finally:
        recorder.close()


def run_source(source: Source, ctx: RunContext) -> Path:
    """Run the full pipeline. Emits to ``output_dir``; delivers a PR unless dry-run."""
    config = source.config
    archive_lineage = config.target in (
        "sentier_inventory",
        "sentier_vocab",
        "sentier_methods",
        "sentier_mappings",
    )
    filename = config.emit_filename or config.name
    out_path = (
        ctx.output_dir
        / config.target
        / config.category
        / f"{filename}{write_mod.EXTENSIONS[config.output_format]}"
    )
    recorder = lineage_mod.LineageRecorder(
        config, lineage_mod.lineage_path(config, ctx), parent_source=type(source), ctx=ctx
    )
    recorder.start_parent([])
    completed = False
    try:
        _, payload, target, validated = _produce(source, ctx, recorder)
        emitted = recorder.artifact(out_path)

        def emit() -> Payload:
            arrow_schema = _arrow_schema(config, target, ctx)
            write_mod.write(payload, out_path, config.output_format, arrow_schema)
            if archive_lineage:
                recorder.describe_artifact(emitted, out_path)
            return payload

        recorder.run_child(
            "emit",
            f"Write the validated payload as {config.output_format}.",
            [validated],
            [emitted],
            emit,
            source=write_mod.write,
        )

        final = emitted
        if ctx.deliver_local_root is not None:
            local_path = (
                ctx.deliver_local_root / target.output_subdir / config.category / out_path.name
            )
            target_ref = lineage_mod.target_dataset(config, target.repo, out_path.name)
            if archive_lineage:
                target_ref = recorder.artifact(local_path, "delivered")

            def deliver() -> Payload:
                deliver_mod.deliver_local(
                    [out_path], target, category=config.category, root=ctx.deliver_local_root
                )
                if archive_lineage:
                    recorder.describe_artifact(target_ref, local_path)
                return payload

            recorder.run_child(
                "deliver",
                "Deliver the emitted artifact to its configured target repository.",
                [emitted],
                [target_ref],
                deliver,
                source=deliver_mod.deliver_local,
            )
            final = target_ref

        recorder.complete_parent([final])
        completed = True
        if archive_lineage:
            inventory_lineage.preserve(out_path, recorder, ctx.lineage_store)
            if ctx.deliver_local_root is not None:
                inventory_lineage.export_bundle(out_path, local_path, ctx.deliver_local_root)
    except BaseException as error:
        if not completed:
            recorder.fail_parent(error)
        raise
    finally:
        recorder.close()
    # A PR packages a completed import. A Git/PR failure must not rewrite that
    # successful history as an import failure or claim publication on main.
    if not ctx.dry_run:
        deliver_mod.deliver(
            [out_path],
            target,
            category=config.category,
            with_lineage=archive_lineage,
            branch=f"import/{config.name}",
            title=f"Import {config.name}",
            body=f"Automated import of {config.name} by sentier_importers.",
            ctx=ctx,
        )
    return out_path

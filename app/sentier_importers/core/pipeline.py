"""Pipeline driver: run a source through
fetch → parse → transform → dedup → assemble → validate → emit → deliver."""

from pathlib import Path

from sentier_importers.core import dedup as dedup_mod
from sentier_importers.core import deliver as deliver_mod
from sentier_importers.core import lineage as lineage_mod
from sentier_importers.core import schema_provider
from sentier_importers.core import validate as validate_mod
from sentier_importers.core import write as write_mod
from sentier_importers.core.context import RunContext
from sentier_importers.core.errors import FetchError, ValidationError
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
    external = [lineage_mod.external_dataset(config.fetch_url)]
    external.extend(lineage_mod.external_dataset(url) for url in config.inputs.values())

    fetched = lineage_mod.intermediate_dataset(config, "fetched-source")
    raw = recorder.run_child(
        "fetch",
        "Fetch and cache the primary source and its named inputs.",
        external,
        [fetched],
        lambda: _fetch(source, ctx),
        source=source.fetch,
    )

    parsed = lineage_mod.intermediate_dataset(config, "parsed-records")
    records = recorder.run_child(
        "parse",
        f"Parse the fetched {config.fetch_format} source into records.",
        [fetched],
        [parsed],
        lambda: source.parse(raw),
        source=source.parse,
    )

    current = parsed
    transformed = records
    for step in source.declared_transform_steps():
        output = lineage_mod.intermediate_dataset(config, step.name)
        transformed = recorder.run_child(
            step.name,
            step.description,
            [current],
            [output],
            lambda step=step, transformed=transformed: step.function(transformed),
            source=step.function,
        )
        current = output

    deduped = lineage_mod.intermediate_dataset(config, "deduplicated-rows")
    rows = recorder.run_child(
        "deduplicate",
        "Deduplicate transformed rows according to the source registry policy.",
        [current],
        [deduped],
        lambda: dedup_mod.dedup(transformed, config, target, ctx),
        source=dedup_mod.dedup,
    )

    assembled = lineage_mod.intermediate_dataset(config, "assembled-payload")
    payload = recorder.run_child(
        "assemble",
        "Assemble rows into the payload shape required by the target.",
        [deduped],
        [assembled],
        lambda: _assemble(rows, config),
        source=_assemble,
    )

    validated = lineage_mod.intermediate_dataset(config, "validated-payload")

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
        config, lineage_mod.lineage_path(config, ctx), parent_source=type(source)
    )
    inputs = [lineage_mod.external_dataset(config.fetch_url)]
    inputs.extend(lineage_mod.external_dataset(url) for url in config.inputs.values())
    recorder.start_parent(inputs)
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
    filename = config.emit_filename or config.name
    out_path = (
        ctx.output_dir
        / config.target
        / config.category
        / f"{filename}{write_mod.EXTENSIONS[config.output_format]}"
    )
    recorder = lineage_mod.LineageRecorder(
        config, lineage_mod.lineage_path(config, ctx), parent_source=type(source)
    )
    inputs = [lineage_mod.external_dataset(config.fetch_url)]
    inputs.extend(lineage_mod.external_dataset(url) for url in config.inputs.values())
    recorder.start_parent(inputs)
    try:
        _, payload, target, validated = _produce(source, ctx, recorder)
        emitted = lineage_mod.local_dataset(out_path)

        def emit() -> Payload:
            arrow_schema = _arrow_schema(config, target, ctx)
            write_mod.write(payload, out_path, config.output_format, arrow_schema)
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
        if ctx.deliver_local_root is not None or not ctx.dry_run:
            target_ref = lineage_mod.target_dataset(config, target.repo, out_path.name)

            def deliver() -> Payload:
                if ctx.deliver_local_root is not None:
                    deliver_mod.deliver_local(
                        [out_path],
                        target,
                        category=config.category,
                        root=ctx.deliver_local_root,
                    )
                if not ctx.dry_run:
                    deliver_mod.deliver(
                        [out_path],
                        target,
                        branch=f"import/{config.name}",
                        title=f"Import {config.name}",
                        body=f"Automated import of {config.name} by sentier_importers.",
                        ctx=ctx,
                    )
                return payload

            recorder.run_child(
                "deliver",
                "Deliver the emitted artifact to its configured target repository.",
                [emitted],
                [target_ref],
                deliver,
                source=(
                    deliver_mod.deliver_local if ctx.deliver_local_root else deliver_mod.deliver
                ),
            )
            final = target_ref

        recorder.complete_parent([final])
        return out_path
    except BaseException as error:
        recorder.fail_parent(error)
        raise
    finally:
        recorder.close()

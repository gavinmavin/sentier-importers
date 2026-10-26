# Import lineage

The normal importer pipeline records OpenLineage events for inventory, vocabulary,
methods and mappings. An import is a parent job; framework stages and declared
source transformations are child jobs connected through their datasets.

```text
fetch -> parse -> source transformations -> deduplicate -> assemble -> validate -> emit
                                                                                -> deliver (optional)
```

Runs stage data locally by default. They do not contact Marquez or open a PR.
Inspection uploads a verified recording and returns a link to its output dataset.

## Setup

Commands below run from the `sentier-importers` checkout. Use Python 3.10+ and uv:

```sh
uv sync --inexact --extra dev
uv run sentier-importers list
```

The four examples use sibling `source_data/` inputs and the `sentier-vocab` checkout's
schemas. These private/licensed inputs are obtained separately and are not committed.
Registry paths remain compatible with the original source layouts; `--input-file`
selects an available local copy without editing the registry or writing a runner.

Marquez must be running when inspecting. The tested local instance exposes its API
at `http://localhost:5000` and UI at `http://localhost:3000`. In an existing Marquez
checkout, `./docker/up.sh --detach` starts its stack; omit `--seed` to start without
sample events. Use that deployment's own shutdown/storage controls when resetting:
both PostgreSQL history and the search index must be cleared for an empty instance.

## Four examples using normal execution

### Inventory: BAFU processes

```sh
uv run sentier-importers run bafu-processes --input-file "../source_data/BAFU-2026 v1_ecoSpold v1.zip"
uv run inspect-lineage --artifact output/sentier_inventory/bafu-2026-v1/processes.parquet --depth 16
```

The standard parser reads the original EcoSpold 1 ZIP. `map-process-rows` emits
11,947 process rows with reference product/unit/amount, sector and source identity.
For the corresponding exchange table, run `bafu-exchanges` with the same input.
XML repairs and EcoSpold 2 conversion are separate from this example.

### Vocabulary: EFSA FoodEx2 qualifiers

```sh
uv run sentier-importers run foodex2-qualifiers --input-file ../source_data/MTX.ecf --schema-dir ../sentier-vocab/schemas
uv run inspect-lineage --artifact output/sentier_vocab/qualifiers/foodex2-qualifiers.parquet --depth 16
```

The source reads EFSA's ECF catalogue, selects qualifiers, maps vocabulary terms and
validates the collection. `foodex2-products` and `foodex2-organisms` use the same input.
Without `--schema-dir`, validation resolves the registry target's schema reference.

### Methods: JRC EF 3.1

```sh
uv run sentier-importers run methods --input-file "../source_data/EF-LCIAMethod_CF(EF-v3.1)__lciamethods_CF.parquet"
uv run sentier-importers run characterization-factors --input-file "../source_data/EF-LCIAMethod_CF(EF-v3.1)__lciamethods_CF.parquet"
uv run inspect-lineage --artifact output/sentier_methods/01-ef-3.1/methods.parquet --depth 16
uv run inspect-lineage --artifact output/sentier_methods/01-ef-3.1/characterization-factors.parquet --depth 16
```

The supplied 319,575-row table produces 25 methods and 319,393 factors. The CF
importer resolves duplicate global factors and harmonises water factors. Its optional
`inputs.simapro` reference is configured in the registry. If unavailable, the existing
land-use fallback applies; the transformation description records its absence.
This can change factor values relative to data produced with the arbiter. It is
not a scientific equivalence claim. The same input also supports `ef-lcia-method`
and `ef-impact-categories`, with `--schema-dir` for their vocabulary validation.

### Mappings: Agribalyse to EF

```sh
uv run sentier-importers run biosphere --input-file ../source_data/mappings_biosphere.parquet --source-provenance agribalyse-3.2-ef-3.1-curated-cut
uv run inspect-lineage --artifact output/sentier_mappings/agribalyse-3.2__ef-3.1/biosphere.json --depth 16
```

The supplied cut contains 1,095 curated/LLM-reviewed entries. The source still runs
its guardrail (allowed targets, no ecoinvent provenance, no identity layer), then maps
replacement entries and assembles a randonneur package. Its output matches the reference
package. The cut's upstream preparation is declared context, not fabricated execution
history. Choose the cut declaration explicitly; the default source describes the full
registry. Unknown publisher, licence and release metadata stay unknown.

## Source and transformation contract

Registry entries select a source module, primary URL/format, target, category,
output format and optional validation contract. Target folders and filenames are
unchanged by lineage. `enabled: false` sources remain runnable by name.

A module exposes one `Source` subclass. Inherited `fetch()` reads/caches the registry
URL and named inputs; override `parse()` for source-specific extraction. Declare
meaningful source operations with `transform_steps()`:

```python
def transform_steps(self):
    return (
        TransformStep(
            "map-process-rows", "Map source records to inventory process rows.",
            self.map_rows,
            inputs=("mapping",),
            code_dependencies=("app/sentier_importers/sources/example/helpers.py",),
        ),
    )
```

Step names are unique lower-kebab identifiers. Framework stage names are reserved.
Each step receives the previous result. `transform()` remains the compatibility entry
point; legacy plugins appear as one `transform` job. An empty step sequence passes
parsed records through. Declare directly used helper files for job-scoped code evidence.

Secondary inputs belong in registry `inputs`. Consume them through `self.inputs[name]`
or `self.input_path(name)` during a step, and declare their roles in `TransformStep.inputs`.
The framework fingerprints consumed bytes/directories, adds upstream graph edges and
replays verified available upstream bundles. Undeclared reads through these interfaces
fail. Untracked filesystem/network reads bypass this contract and must not be used
for dependencies that affect output. Missing upstream history is `not-recorded`.

## Source provenance contract

Registry `source_datasets` defines reusable dataset descriptions. A source's
`provenance` maps its primary role `source` and every declared secondary input role
to those descriptions. Existing sources can adopt this incrementally. Once opted in,
invalid references, missing roles and malformed metadata fail registry loading.

Required keys are `source`, `source_version`, `title`, `publisher`, `license` and
`citation`; unknown values can be null except source ID/title. Optional fields are
`creators`, publication `date`, `identifier`, public `source_url`, `license_url`,
`format`, `format_version` and textual `coverage`. Vocabulary field meanings follow
`sentier-vocab`. Public reference URLs must exclude credentials/query tokens.

The framework freezes the resolved declaration in `sentier_source` alongside the
observed SHA-256, byte size, read/observation timestamps and acquisition method.
Only a fresh HTTP acquisition records `downloadedAt`; cache/local reads do not infer
a historical download date. Import time is never substituted for publication time.
Provider attribution is separate from the optional public run contributor.

`--source-provenance <key>` selects an existing declaration for an explicitly chosen
input; it does not create one or infer its correctness from a filename. Named inputs
must already have provenance declarations. `--input-file` and `--source-provenance`
apply to one source, not `--all`.

## Recordings, inspection and privacy

- Every job emits `START` then `COMPLETE`, or `FAIL`. Exceptions are re-raised;
  failure facets exclude raw exception messages/tracebacks that may reveal local data.
- Working JSONL defaults to `output/lineage/<source>.openlineage.jsonl`. Repeated
  runs replace that working file; successful dry-run outputs retain immutable history.
- `.lineage/<filename>.json` points to a version-2 bundle containing `events.jsonl`,
  `manifest.json` and verified upstream bundles. Defaults live in `.lineage/<run-id>/`;
  `--lineage-store` relocates the store. Keep pointers, artifacts and referenced bundles
  together, or update the local store reference when relocating them.
- Inspection verifies artifact/event checksums and successful run linkage, uploads
  upstream events in order, checks the graph, and prints its URL. It does not re-import
  data. Legacy data without a compatible bundle reports no lineage information.
- Dataset names are filenames. Abstract namespaces distinguish workspace, target,
  release, run and SHA-256 without exporting physical paths, OS users or hostnames.
- Code facets include the actual transformation callable, relative implementation
  paths, file hashes, Git commit and job-scoped `codeDirty`. A dirty source link points
  to the committed baseline; captured source/hashes describe the local execution.
- Schemas/counts, transformation statistics, up to five row previews, and three distinct
  non-null column examples support inspection. This is not arbitrary row tracing.
  Object-valued mapping `source`/`target` preview cells use declared JSON text encoding
  to avoid Marquez search-index conflicts; emitted data/schema types stay structured.

Only explicitly provided `--contributor <public-id>` is recorded as a contributor.
Keep secrets, personal details and sensitive paths out of captured/committed code.
Previews contain real source values and must be handled under the data's terms.
Facet JSON schemas in `facets/` are implementation contracts; generated events,
data, cache files and local development material are not committed.

Inspection defaults to depth 8; `--depth` overrides it. API/UI locations are configurable
with `--marquez-api` and `--marquez-web`. Marquez lists are namespace-filtered; the
printed dataset link avoids navigating the wrong namespace.

Validation enforces only the configured schema. These commands stage example files;
complete release metadata, target-repository validation and publication are separate
steps. Remote PR delivery behavior is unchanged by lineage; published lineage
distribution and Brightway integration remain outside these local examples.

## Checks

```sh
uv run pytest tests
uv run ruff check app
```

Tests use synthetic input and temporary recordings. `--output-dir`, `--cache-dir`,
`--lineage-dir`, `--lineage-store` and `--schema-dir` can isolate local work. Neither
unit tests nor a normal `run` populate Marquez; only inspection sends events.
On Windows, choose a short test temporary directory or lineage-store path when
testing deeply nested upstream bundles to stay within filesystem path limits.

"""Build the isolated sentier-inventory/examples/98-test teaching example.

Run from anywhere: python scripts/run_bafu_test_example.py
The BAFU ZIP remains under the workspace's bafu-test/ directory.
"""

import hashlib
import json
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq

IMPORTERS = Path(__file__).resolve().parents[1]
WORKSPACE = IMPORTERS.parent
ZIP = WORKSPACE / "bafu-test" / "BAFU_test.zip"
DEST = WORKSPACE / "sentier-inventory" / "examples" / "98-test"

sys.path.insert(0, str(IMPORTERS / "app"))

from sentier_importers.core import pipeline, registry  # noqa: E402
from sentier_importers.core.context import RunContext  # noqa: E402


def main() -> None:
    if not ZIP.is_file():
        raise FileNotFoundError(ZIP)
    digest = hashlib.sha256(ZIP.read_bytes()).hexdigest()
    ctx = RunContext(
        cache_dir=IMPORTERS / "cache" / f"bafu-test-{digest[:12]}",
        output_dir=IMPORTERS / "output" / "bafu-test",
    )
    paths = []
    for name in ("bafu-test-processes", "bafu-test-exchanges"):
        config = replace(registry.get_config(name), fetch_url=f"file://{ZIP}")
        paths.append(pipeline.run_source(registry.load_source(config), ctx))

    DEST.mkdir(parents=True, exist_ok=True)
    for path in paths:
        shutil.copy2(path, DEST / path.name)
    counts = {path.name: pq.read_metadata(path).num_rows for path in paths}
    metadata = {
        "sector": "test",
        "title": "BAFU test example",
        "description": "Isolated teaching example from bafu-test/BAFU_test.zip; infrastructure processes excluded.",
        "rank": 98,
        "schema_version": "0.1.0",
        "row_counts": counts,
    }
    (DEST / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Source SHA-256: {digest}")
    print(f"Wrote {DEST}: {counts}")


if __name__ == "__main__":
    main()

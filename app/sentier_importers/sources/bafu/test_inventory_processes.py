"""Example BAFU ZIP -> inventory processes for the isolated 98-test sector."""

from sentier_importers.core.source import Source, TransformStep
from sentier_importers.core.types import RawData, Records, Rows
from sentier_importers.sources.bafu import ecospold
from sentier_importers.sources.bafu.test_example import normalized_name, selected_records


class BafuTestProcessesSource(Source):
    def parse(self, raw: RawData) -> Records:
        return ecospold.parse_ecospold_zip(raw)

    def transform(self, records: Records) -> Rows:
        """Compatibility entry point; the pipeline uses ``transform_steps``."""
        return self._map_process_rows(selected_records(records))

    def transform_steps(self) -> tuple[TransformStep, ...]:
        return (
            TransformStep(
                name="filter-infrastructure",
                description="Drop infrastructure processes from the BAFU test records.",
                function=selected_records,
            ),
            TransformStep(
                name="map-process-rows",
                description=(
                    "Normalize process names and map BAFU records to Sentier inventory rows."
                ),
                function=self._map_process_rows,
            ),
        )

    def _map_process_rows(self, records: Records) -> Rows:
        rows: Rows = []
        for record in records:
            name = normalized_name(record["name"])
            rows.append(
                {
                    "process_id": record["uuid"],
                    "name": name,
                    "reference_product": name,
                    "reference_unit": record["unit"],
                    "reference_amount": record["amount"],
                    "location": record["location"],
                    "process_type": "unit",
                    "technology": record["technology"],
                    "comment": (
                        f"BAFU test example; category: {record['category']} / "
                        f"{record['subcategory']}. Source: {ecospold.CITATION}"
                    ),
                }
            )
        return rows

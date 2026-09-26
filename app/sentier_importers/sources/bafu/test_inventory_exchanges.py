"""Example BAFU ZIP -> exchanges belonging to the retained 98-test processes."""

from sentier_importers.core.source import Source
from sentier_importers.core.types import RawData, Records, Rows
from sentier_importers.sources.bafu import ecospold
from sentier_importers.sources.bafu.inventory_exchanges import _flow_type
from sentier_importers.sources.bafu.test_example import selected_records


class BafuTestExchangesSource(Source):
    def parse(self, raw: RawData) -> Records:
        return ecospold.parse_ecospold_zip(raw)

    def transform(self, records: Records) -> Rows:
        # Resolve technosphere links against the whole ZIP, including filtered records.
        number_to_uuid = {record["number"]: record["uuid"] for record in records}
        rows: Rows = []
        for record in selected_records(records):
            for exchange in record["exchanges"]:
                flow_type = _flow_type(exchange["group"], exchange["group_code"])
                if flow_type == "production":
                    flow = record["uuid"]
                elif flow_type == "biosphere":
                    flow = ecospold.flow_id(
                        exchange["name"],
                        exchange["category"],
                        exchange["subcategory"],
                        exchange["unit"],
                    )
                else:
                    flow = number_to_uuid.get(exchange["number"], exchange["number"])
                uncertainty_type, loc, scale = ecospold.bw_uncertainty(
                    exchange["amount"], exchange["uncertainty_type"], exchange["sd95"]
                )
                rows.append(
                    {
                        "process_id": record["uuid"],
                        "flow": flow,
                        "flow_name": exchange["name"],
                        "flow_type": flow_type,
                        "direction": exchange["group"],
                        "amount": exchange["amount"],
                        "unit": exchange["unit"],
                        "location": exchange["location"],
                        "uncertainty_type": uncertainty_type,
                        "loc": loc,
                        "scale": scale,
                        "minimum": None,
                        "maximum": None,
                    }
                )
        return rows

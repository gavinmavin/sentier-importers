"""Small, deliberate transformations for the isolated BAFU 98-test example."""

from sentier_importers.core.types import Records


def selected_records(records: Records) -> Records:
    """Drop infrastructure processes to demonstrate a visible filter (10 -> 8)."""
    return [record for record in records if not record["infrastructure"]]


def normalized_name(value: str) -> str:
    """Collapse incidental whitespace without changing source identifiers or units."""
    return " ".join(value.split())

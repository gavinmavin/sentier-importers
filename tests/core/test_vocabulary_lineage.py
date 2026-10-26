import json
from dataclasses import replace

import httpx
import pytest
from sentier_importers import __main__ as cli
from sentier_importers.core import inventory_lineage, lineage, pipeline

from tests.core.test_pipeline import COLLECTION_SCHEMA, _ctx, _product_source


@pytest.mark.parametrize("output_format", ["yaml", "parquet"])
@pytest.mark.parametrize("deliver", [False, True])
def test_vocabulary_outputs_have_versioned_local_bundles(
    tmp_path, monkeypatch, output_format, deliver
):
    root = tmp_path / "vocab"
    root.mkdir()
    monkeypatch.setattr(
        pipeline.schema_provider, "resolve_schema", lambda *args: COLLECTION_SCHEMA
    )
    ctx = _ctx(tmp_path)
    if deliver:
        ctx = replace(ctx, deliver_local_root=root)
    staged = pipeline.run_source(
        _product_source(output_format=output_format, emit_filename="foodex2"), ctx
    )
    artifact = root / "data" / "products" / staged.name if deliver else staged
    manifest, events = inventory_lineage.read_bundle(artifact)
    assert manifest["sha256"] == lineage.file_sha256(artifact)
    assert manifest["dataset"]["name"] == artifact.name
    assert "/sentier-vocab/products/" in manifest["dataset"]["namespace"]
    assert ("/delivered/" in manifest["dataset"]["namespace"]) is deliver
    assert manifest["dataset"]["facets"]["sentier_preview"]["rowCount"] == 2
    assert all(str(root).replace("\\", "\\\\") not in json.dumps(event) for event in events)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"graph": [{"id": "output"}]})

    real_client = httpx.Client
    monkeypatch.setattr(
        inventory_lineage.httpx,
        "Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    cli.main(["inspect-lineage", "--artifact", str(artifact), "--depth", "12"])
    assert requests[-1].url.params["depth"] == "12"
    assert artifact.name in requests[-1].url.params["nodeId"]


def test_legacy_vocabulary_artifact_does_not_contact_marquez(tmp_path, monkeypatch, capsys):
    def unexpected(**kwargs):
        raise AssertionError("No lineage must not contact Marquez")

    monkeypatch.setattr(inventory_lineage.httpx, "Client", unexpected)
    cli.main(["inspect-lineage", "--artifact", str(tmp_path / "foodex2.parquet")])
    assert (
        "No lineage information available for this endpoint: foodex2.parquet"
        in capsys.readouterr().out
    )


@pytest.mark.parametrize(
    "selection",
    [
        ["--processes"],
        ["demo-v1", "--artifact", "foodex2.parquet"],
        ["demo-v1", "--processes", "--artifact", "foodex2.parquet"],
    ],
)
def test_inventory_and_artifact_selectors_cannot_be_confused(selection):
    with pytest.raises(SystemExit) as exc:
        cli.main(["inspect-lineage", *selection])
    assert exc.value.code != 0

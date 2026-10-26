"""Durable local artifact provenance and shared Marquez inspection."""

import argparse
import copy
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import httpx
import pyarrow.parquet as pq
from sentier_importers.core.errors import SentierImporterError
from sentier_importers.core.lineage import DatasetRef, external_dataset, file_sha256
from sentier_importers.core.types import RawData


def _write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _contained(root, relative):
    if not isinstance(relative, str):
        raise ValueError("Lineage path must be a string")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Lineage reference escapes its bundle directory")
    return path


def _load_archive(path, active=None):
    active = set() if active is None else active
    key = path.resolve()
    if key in active:
        raise ValueError("Cycle in upstream lineage bundles")
    if len(active) >= 64:
        raise ValueError("Upstream lineage nesting exceeds 64 runs")
    active.add(key)
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest["formatVersion"] != 2:
            raise ValueError("Unsupported upstream lineage format")
        event_path = _contained(path.parent, manifest["events"])
        if file_sha256(event_path) != manifest["eventsSha256"]:
            raise ValueError("Lineage event checksum differs from the manifest")
        events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
        terminal = events[-1]
        if (
            terminal["eventType"] != "COMPLETE"
            or terminal["run"]["runId"] != manifest["runId"]
            or terminal["job"]["name"] != manifest["job"]
            or terminal["outputs"] != [manifest["dataset"]]
            or manifest["dataset"]["facets"]["version"]["datasetVersion"]
            != f"sha256:{manifest['sha256']}"
            or any(event["eventType"] == "FAIL" for event in events)
        ):
            raise ValueError("Lineage does not describe a matching successful output run")
        children = []
        for dependency in manifest.get("dependencies", []):
            if dependency["status"] != "available":
                continue
            child = _load_archive(_contained(path.parent, dependency["manifest"]), active)
            upstream = child["manifest"]
            if (
                upstream["sha256"] != dependency["sha256"]
                or upstream["runId"] != dependency["runId"]
                or upstream["dataset"]["namespace"] != dependency["dataset"]["namespace"]
                or upstream["dataset"]["name"] != dependency["dataset"]["name"]
                or upstream["eventsSha256"] != dependency["eventsSha256"]
            ):
                raise ValueError("Mismatched upstream provenance reference")
            children.append(child)
        return {"manifest": manifest, "events": events, "children": children}
    finally:
        active.remove(key)


def _bundle_snapshot(artifact):
    pointer = artifact.parent / ".lineage" / f"{artifact.name}.json"
    if not pointer.exists():
        return None
    try:
        index = json.loads(pointer.read_text(encoding="utf-8"))
        store = (pointer.parent / index.get("store", ".")).resolve()
        path = _contained(store, index["manifest"])
        header = json.loads(path.read_text(encoding="utf-8"))
        if header.get("formatVersion") == 1:
            return None  # prior recordings may expose local identities; never replay them
        snapshot = _load_archive(path)
        manifest = snapshot["manifest"]
        if manifest["artifact"] != artifact.name or file_sha256(artifact) != manifest["sha256"]:
            raise ValueError("Artifact checksum differs from the recorded run")
        return snapshot
    except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
        raise SentierImporterError(f"Cannot inspect lineage for {artifact.name}: {exc}") from exc


def read_bundle(artifact):
    snapshot = _bundle_snapshot(artifact)
    return (snapshot["manifest"], snapshot["events"]) if snapshot else None


def _write_archive(folder, snapshot):
    folder.mkdir(parents=True, exist_ok=True)
    manifest = copy.deepcopy(snapshot["manifest"])
    for child in snapshot["children"]:
        child_id = child["manifest"]["runId"]
        _write_archive(folder / "upstream" / child_id, child)
    (folder / "events.jsonl").write_text(
        "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in snapshot["events"]),
        encoding="utf-8",
        newline="\n",
    )
    manifest["eventsSha256"] = file_sha256(folder / "events.jsonl")
    # Event serialization is canonical across copied archives.
    for dependency in manifest.get("dependencies", []):
        if dependency["status"] == "available":
            child_path = folder / dependency["manifest"]
            dependency["eventsSha256"] = json.loads(child_path.read_text(encoding="utf-8"))[
                "eventsSha256"
            ]
    _write_json(folder / "manifest.json", manifest)
    return manifest


def preserve(artifact, recorder, store=None):
    recorder.flush()
    events = [json.loads(line) for line in recorder.path.read_text(encoding="utf-8").splitlines()]
    terminal = events[-1]
    if terminal["eventType"] != "COMPLETE":
        raise SentierImporterError("Cannot preserve an incomplete artifact lineage run")
    dependencies, children = [], []
    for dependency in recorder.dependencies.values():
        item = {key: value for key, value in dependency.items() if key != "snapshot"}
        snapshot = dependency.get("snapshot")
        if snapshot:
            upstream = snapshot["manifest"]
            item.update(
                manifest=f"upstream/{upstream['runId']}/manifest.json",
                runId=upstream["runId"],
                eventsSha256=upstream["eventsSha256"],
            )
            children.append(snapshot)
        dependencies.append(item)
    manifest = {
        "formatVersion": 2,
        "artifact": artifact.name,
        "sha256": file_sha256(artifact),
        "runId": recorder.parent_run_id,
        "job": terminal["job"]["name"],
        "dataset": terminal["outputs"][0],
        "completedAt": terminal["eventTime"],
        "execution": recorder.execution,
        "events": "events.jsonl",
        "dependencies": dependencies,
    }
    pointer_dir = artifact.parent / ".lineage"
    pointer_dir.mkdir(parents=True, exist_ok=True)
    store = store.resolve() if store else pointer_dir.resolve()
    folder = store / recorder.parent_run_id
    _write_archive(folder, {"manifest": manifest, "events": events, "children": children})
    try:
        store_reference = os.path.relpath(store, pointer_dir)
    except ValueError:
        # Windows stores on another drive need a private absolute locator.
        store_reference = str(store)
    _write_json(
        pointer_dir / f"{artifact.name}.json",
        {
            "store": store_reference,
            "manifest": f"{recorder.parent_run_id}/manifest.json",
        },
    )


def local_input_path(url):
    if not url.startswith("file://"):
        return None
    value = unquote(url[len("file://") :]).replace("\\", "/")
    if len(value) >= 4 and value[0] == "/" and value[2] == ":":
        value = value[1:]
    return Path(value)


def observe_dependency(recorder, role, url, value):
    """Freeze observed bytes and verified upstream history at consumption time."""
    # Physical locators stay in process memory, not in shareable events/manifests.
    path = value if isinstance(value, Path) else local_input_path(url)
    files = None
    if isinstance(value, RawData):
        digest = hashlib.sha256(value.content).hexdigest()
    elif path and path.exists():
        digest, files = path_fingerprint(path)
    else:
        raise SentierImporterError(f"Cannot observe declared input {role!r}")
    snapshot = _bundle_snapshot(path) if path and path.is_file() else None
    if snapshot and snapshot["manifest"]["sha256"] != digest:
        raise SentierImporterError(f"Consumed input {role!r} differs from its lineage bundle")
    if snapshot:
        dataset = snapshot["manifest"]["dataset"]
        ref = DatasetRef(dataset["namespace"], dataset["name"], version=f"sha256:{digest}")
        status = "available"
    else:
        ref = external_dataset(url)
        ref.namespace = (
            f"sentier://{'local' if path else 'remote'}/{recorder.workspace_id}/"
            f"inputs/{role}/sha256/{digest}"
        )
        ref.version = f"sha256:{digest}"
        ref.description = "Upstream lineage not recorded for this version."
        status = "not-recorded"
    ref.provenance = {"status": status, "role": role}
    metadata = recorder.config.input_provenance.get(role)
    if metadata:
        observed_at = datetime.now(timezone.utc).isoformat()
        method = value.acquisition_method if isinstance(value, RawData) else "direct-file"
        read_at = value.read_at if isinstance(value, RawData) else observed_at
        observation = {
            "sha256": digest,
            "sizeBytes": (
                len(value.content)
                if isinstance(value, RawData)
                else (path.stat().st_size if path.is_file() else None)
            ),
            "observedAt": observed_at,
            "readAt": read_at,
            "acquisitionMethod": method,
            "downloadedAt": read_at if method == "download" else None,
        }
        ref.source_metadata = {"metadata": copy.deepcopy(metadata), "observation": observation}
        ref.description = f"{metadata['title']}."
        if status == "not-recorded":
            ref.description += " Upstream lineage not recorded for this version."
    if files is not None:
        ref.provenance["files"] = files
    if isinstance(value, RawData) and (path and path.suffix == ".parquet"):
        ref.capture(pq.read_table(io.BytesIO(value.content)).to_pylist())
    key = (role, ref.namespace, ref.name)
    recorder.dependencies[key] = {
        "role": role,
        "status": status,
        "sha256": digest,
        "dataset": {"namespace": ref.namespace, "name": ref.name},
        "snapshot": snapshot,
    }
    if ref.source_metadata:
        recorder.dependencies[key]["sourceMetadata"] = copy.deepcopy(ref.source_metadata)
    return ref


def path_fingerprint(path):
    if path.is_file():
        return file_sha256(path), None
    files = [
        {"name": p.relative_to(path).as_posix(), "sha256": file_sha256(p)}
        for p in sorted(path.rglob("*"))
        if p.is_file() and ".lineage" not in p.relative_to(path).parts
    ]
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(), files


def positive_depth(value):
    depth = int(value)
    if depth < 1:
        raise argparse.ArgumentTypeError("Depth must be a positive integer")
    return depth


def inspect_inventory(source, endpoint, inventory_root, api_url, web_url, depth=8):
    if not isinstance(depth, int) or depth < 1:
        raise SentierImporterError("Depth must be a positive integer")
    if Path(source).name != source or source in (".", "..") or "/" in source or "\\" in source:
        raise SentierImporterError("Source must be an inventory source folder name")
    data_root = inventory_root / "data" if (inventory_root / "data").is_dir() else inventory_root
    artifact = data_root / source / f"{endpoint}.parquet"
    return inspect_artifact(
        artifact, api_url, web_url, depth, label=f"{source}/{endpoint}.parquet"
    )


def inspect_artifact(artifact, api_url, web_url, depth=8, *, label=None):
    """Inspect a recorded local output without repository-specific endpoint rules."""
    if not isinstance(depth, int) or depth < 1:
        raise SentierImporterError("Depth must be a positive integer")
    artifact = Path(artifact)
    snapshot = _bundle_snapshot(artifact)
    if snapshot is None:
        print(f"No lineage information available for this endpoint: {label or artifact.name}")
        return None
    ordered, seen = [], {}

    def visit(bundle):
        manifest = bundle["manifest"]
        key = manifest["runId"]
        if key in seen:
            if seen[key] != manifest["eventsSha256"]:
                raise SentierImporterError("Conflicting upstream run records")
            return
        for child in bundle["children"]:
            visit(child)
        seen[key] = manifest["eventsSha256"]
        ordered.extend(bundle["events"])

    visit(snapshot)
    for address in (api_url, web_url):
        if urlsplit(address).scheme not in ("http", "https"):
            raise SentierImporterError("Marquez addresses must use http:// or https://")
    try:
        with httpx.Client(base_url=api_url.rstrip("/"), timeout=60, trust_env=False) as client:
            for event in ordered:
                client.post("/api/v1/lineage", json=event).raise_for_status()
            dataset = snapshot["manifest"]["dataset"]
            response = client.get(
                "/api/v1/lineage",
                params={
                    "nodeId": f"dataset:{dataset['namespace']}:{dataset['name']}",
                    "depth": depth,
                },
            )
            response.raise_for_status()
            if not response.json().get("graph"):
                raise SentierImporterError(
                    "Marquez accepted the events but returned an empty graph"
                )
    except (httpx.HTTPError, ValueError) as exc:
        raise SentierImporterError(
            f"Could not load lineage into Marquez at {api_url}. "
            "Check that the API is running and the address is correct."
        ) from exc
    manifest = snapshot["manifest"]
    url = (
        f"{web_url.rstrip('/')}/lineage/dataset/"
        f"{quote(dataset['namespace'], safe='')}/{quote(dataset['name'], safe='')}?depth={depth}"
    )
    print(f"Verified artifact: {artifact.resolve()}")
    print(f"Producing run: {manifest['runId']}; completed: {manifest['completedAt']}")
    if manifest["execution"].get("contributor"):
        print(f"Contributor: {manifest['execution']['contributor']}")
    print(f"Marquez: {url}")
    return url


def add_arguments(parser):
    parser.add_argument("source", nargs="?", help="Inventory source folder, e.g. bafu-2026-v1")
    endpoints = parser.add_mutually_exclusive_group(required=True)
    endpoints.add_argument("--processes", dest="endpoint", action="store_const", const="processes")
    endpoints.add_argument("--exchanges", dest="endpoint", action="store_const", const="exchanges")
    endpoints.add_argument(
        "--artifact",
        type=Path,
        help="Recorded local artifact, e.g. a vocabulary Parquet or YAML file",
    )
    parser.add_argument(
        "--inventory-root",
        type=Path,
        default=Path(__file__).resolve().parents[4] / "sentier-inventory",
        help="Inventory checkout or staging directory",
    )
    parser.add_argument("--marquez-api", default="http://localhost:5000")
    parser.add_argument("--marquez-web", default="http://localhost:3000")
    parser.add_argument("--depth", type=positive_depth, default=8)


def command(args):
    if args.artifact is not None:
        if args.source is not None:
            raise SentierImporterError(
                "Use --artifact by itself, or select an inventory source and endpoint"
            )
        return inspect_artifact(args.artifact, args.marquez_api, args.marquez_web, args.depth)
    if args.source is None:
        raise SentierImporterError("An inventory endpoint requires a source folder")
    inspect_inventory(
        args.source,
        args.endpoint,
        args.inventory_root,
        args.marquez_api,
        args.marquez_web,
        args.depth,
    )


def main():
    parser = argparse.ArgumentParser(prog="inspect-lineage")
    add_arguments(parser)
    try:
        command(parser.parse_args())
    except SentierImporterError as exc:
        parser.exit(1, f"error: {exc}\n")

"""Deliver stage: clone the target repo, branch, copy emitted files into the target's
path, commit, push, and open a PR via ``gh``. A no-op in dry-run mode.

Delivery copies over whatever the target already holds at the same path, no merge.
For inventory targets the category folder is the source folder (``data/<source>-
<source_version>/``), so a full overwrite is the intended semantics: a run for a
source rewrites its own folder whole, and sources never share a folder or a file.
"""

import shutil
import subprocess
from pathlib import Path, PurePosixPath

from sentier_importers.core import inventory_lineage
from sentier_importers.core.context import RunContext
from sentier_importers.core.errors import DeliveryError
from sentier_importers.core.targets import Target


def _run(cmd: list[str], cwd: Path | None = None) -> str:
    """Run a subprocess, returning stdout. Raises CalledProcessError on non-zero exit."""
    result = subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
    return result.stdout


def _remove_stale_same_stem(dest: Path) -> None:
    """Delete any sibling of ``dest`` with the same stem but a different extension.

    When a source's output format changes (e.g. ``foodex2.yaml`` -> ``foodex2.parquet``),
    this prevents the target repo from ending up with both the old and new files.
    """
    for sibling in dest.parent.glob(f"{dest.stem}.*"):
        if sibling.name != dest.name:
            bundle = inventory_lineage._current_folder(sibling)
            if bundle.is_dir():
                shutil.rmtree(bundle)
            sibling.unlink()
            pointer = dest.parent / ".lineage" / f"{sibling.name}.json"
            if pointer.is_file():
                pointer.unlink()


def _destination(root, target, category):
    for value in (target.output_subdir, category):
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or "\\" in value or ":" in value:
            raise DeliveryError("Delivery folders must be portable repository-relative paths")
    root = root.resolve()
    destination = (root / target.output_subdir / category).resolve()
    if not destination.is_relative_to(root):
        raise DeliveryError("Delivery folder escapes its repository")
    return destination


def deliver_local(files: list[Path], target: Target, *, category: str, root: Path) -> list[Path]:
    """Copy emitted ``files`` into a local checkout of ``target`` — no git, no PR.

    Places each file at ``root/<output_subdir>/<category>/<name>``, preserving the
    category folder. This is the
    regenerate-locally delivery path for sources whose data cannot be pushed to a
    public repo yet; ``root`` must be an existing directory. Existing files at the
    destination are overwritten: for inventory targets the category is the source
    folder, which a delivery is meant to replace whole.
    """
    if not root.is_dir():
        raise DeliveryError(f"local delivery root is not a directory: {root}")
    dest_dir = _destination(root, target, category)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dests: list[Path] = []
    for file in files:
        dest = dest_dir / file.name
        _remove_stale_same_stem(dest)
        shutil.copy2(file, dest)
        dests.append(dest)
    return dests


def deliver(
    files: list[Path],
    target: Target,
    *,
    branch: str,
    title: str,
    body: str,
    ctx: RunContext,
    category: str = "",
    with_lineage: bool = False,
) -> str | None:
    """Open a PR adding ``files`` to ``target``. Returns the PR URL, or None in dry-run.

    In dry-run mode this performs no git/gh operations. Otherwise it clones the
    target into a working dir under the cache, creates ``branch``, copies the files
    into ``target.output_subdir/category``. With lineage, it also exports portable
    current bundles and byte-preserving attributes into the same commit,
    then pushes and runs ``gh pr create``.
    """
    if ctx.dry_run:
        return None

    cache_root = ctx.cache_dir.resolve()
    delivery_root = (cache_root / "delivery").resolve()
    if not delivery_root.is_relative_to(cache_root):
        raise DeliveryError("Delivery checkout root escapes its cache")
    workdir = (delivery_root / target.name).resolve()
    if workdir.parent != delivery_root:
        raise DeliveryError("Delivery checkout must be a direct child of the delivery cache")
    _destination(workdir, target, category)  # Validate before touching the checkout.
    if with_lineage:
        for file in files:
            if inventory_lineage.read_bundle(file) is None:
                raise DeliveryError(f"No completed lineage bundle for {file.name}")
    try:
        if workdir.exists():
            shutil.rmtree(workdir)
        workdir.parent.mkdir(parents=True, exist_ok=True)
        _run(["git", "clone", "--depth", "1", target.repo, str(workdir)])
        _run(["git", "checkout", "-b", branch], cwd=workdir)

        destinations = deliver_local(files, target, category=category, root=workdir)
        lineage_paths = []
        if with_lineage:
            for source, destination in zip(files, destinations):
                lineage_paths.extend(inventory_lineage.export_bundle(source, destination, workdir))

        _run(["git", "add", "--", target.output_subdir], cwd=workdir)
        if lineage_paths:
            # Force only the verified current bundles if a target ignores
            # generated lineage globally; never force-add the contributor's store.
            _run(
                [
                    "git",
                    "add",
                    "-f",
                    "--",
                    ".gitattributes",
                    *[path.relative_to(workdir).as_posix() for path in lineage_paths],
                ],
                cwd=workdir,
            )
        _run(["git", "commit", "-m", title], cwd=workdir)
        _run(["git", "push", "-u", "origin", branch], cwd=workdir)
        url = _run(["gh", "pr", "create", "--title", title, "--body", body], cwd=workdir)
        return url.strip()
    except subprocess.CalledProcessError as exc:
        raise DeliveryError(f"delivery to {target.name} failed: {exc}") from exc

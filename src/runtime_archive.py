"""Hydrate a runtime configuration archive from a local path or URL."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence
from urllib.parse import urlparse


REQUIRED_FILES = ("pipelines/manifest.json",)

# Schemes the engine fetches over the network. A presigned object-store URL is
# opaque HTTPS, so accepting these keeps the engine cloud-agnostic: no cloud SDK,
# just urllib downloading bytes the same way it would from any other URL.
_REMOTE_SCHEMES = frozenset({"http", "https"})
_DOWNLOAD_TIMEOUT_SECONDS = 60


class RuntimeArchiveError(RuntimeError):
    """Raised when a runtime archive cannot be hydrated."""


def hydrate_archive(archive_path: Path | str, destination: Path | str = ".") -> Path:
    """Extract a runtime archive into ``destination`` and validate the layout.

    ``archive_path`` is either a local filesystem path or an ``http(s)://`` URL
    (for example a presigned object-store URL), which is downloaded to a
    temporary file first. The archive carries the same directory layout the
    engine reads during normal local execution, including
    ``pipelines/manifest.json``.
    """

    target_dir = Path(destination).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    with _local_archive(archive_path) as archive:
        _extract_tar_safely(archive, target_dir)
    _validate_layout(target_dir)
    return target_dir


@contextmanager
def _local_archive(archive_path: Path | str) -> Iterator[Path]:
    """Yield a local path to the archive, downloading it first when it is a URL."""

    if _is_remote_url(archive_path):
        url = str(archive_path)
        handle, tmp_name = tempfile.mkstemp(suffix=".tar")
        os.close(handle)
        tmp_path = Path(tmp_name)
        try:
            _download_archive(url, tmp_path)
            yield tmp_path
        finally:
            tmp_path.unlink(missing_ok=True)
    else:
        archive = Path(archive_path).expanduser()
        if not archive.is_file():
            raise RuntimeArchiveError(f"Runtime archive not found: {archive}")
        yield archive


def _is_remote_url(archive_path: Path | str) -> bool:
    return (
        isinstance(archive_path, str)
        and urlparse(archive_path).scheme in _REMOTE_SCHEMES
    )


def _download_archive(url: str, destination: Path) -> None:
    # _is_remote_url has already constrained the scheme to http(s), so urlopen
    # cannot be steered at file:// or another local-resource scheme here.
    try:
        with urllib.request.urlopen(  # noqa: S310 - scheme allowlisted above
            url, timeout=_DOWNLOAD_TIMEOUT_SECONDS
        ) as response, destination.open("wb") as out_file:
            shutil.copyfileobj(response, out_file)
    except urllib.error.HTTPError as exc:
        # An expired or revoked presigned URL fails here. Report the HTTP status
        # so the operator is not sent chasing a "corrupt archive" at extraction.
        raise RuntimeArchiveError(
            f"Runtime archive download returned HTTP {exc.code}: {url}"
        ) from exc
    except OSError as exc:
        raise RuntimeArchiveError(
            f"Failed to download runtime archive: {url}: {exc}"
        ) from exc

    # An empty 200 body would otherwise surface as an opaque tar read error.
    if destination.stat().st_size == 0:
        raise RuntimeArchiveError(
            f"Runtime archive download produced an empty file: {url}"
        )


def _extract_tar_safely(archive_path: Path, destination: Path) -> None:
    try:
        with tarfile.open(archive_path, mode="r:*") as archive:
            members = archive.getmembers()
            for member in members:
                _validate_member(member, destination)
            archive.extractall(destination, members=members, filter="data")
    except tarfile.TarError as exc:
        raise RuntimeArchiveError(
            f"Invalid runtime archive {archive_path}: {exc}"
        ) from exc
    except OSError as exc:
        raise RuntimeArchiveError(
            f"Failed to extract runtime archive into {destination}: {exc}"
        ) from exc


def _validate_member(member: tarfile.TarInfo, destination: Path) -> None:
    member_path = Path(member.name)

    if member_path.is_absolute():
        raise RuntimeArchiveError(f"Archive contains absolute path: {member.name}")
    if ".." in member_path.parts:
        raise RuntimeArchiveError(f"Archive contains parent traversal: {member.name}")
    if member.issym() or member.islnk():
        raise RuntimeArchiveError(f"Archive contains unsupported link: {member.name}")
    if not (member.isfile() or member.isdir()):
        raise RuntimeArchiveError(f"Archive contains unsupported entry: {member.name}")

    resolved = (destination / member.name).resolve()
    try:
        resolved.relative_to(destination)
    except ValueError as exc:
        raise RuntimeArchiveError(
            f"Archive entry escapes destination: {member.name}"
        ) from exc


def _validate_layout(destination: Path) -> None:
    missing = [path for path in REQUIRED_FILES if not (destination / path).is_file()]
    if missing:
        raise RuntimeArchiveError(
            "Runtime archive is missing required file(s): " + ", ".join(missing)
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.runtime_archive",
        description="Hydrate a local Analitiq runtime configuration archive.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    hydrate = subcommands.add_parser("hydrate", help="Extract an archive and exit.")
    hydrate.add_argument("archive", help="Local path or http(s):// URL of the archive.")
    hydrate.add_argument(
        "-C",
        "--destination",
        type=Path,
        default=Path.cwd(),
        help="Directory to hydrate into. Defaults to the current working directory.",
    )

    run = subcommands.add_parser("run", help="Extract an archive, then run a command.")
    run.add_argument("archive", help="Local path or http(s):// URL of the archive.")
    run.add_argument(
        "-C",
        "--destination",
        type=Path,
        default=Path.cwd(),
        help="Directory to hydrate into. Defaults to the current working directory.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    parser_args, exec_command = _split_exec_command(raw_args)

    parser = _build_parser()
    args = parser.parse_args(parser_args)

    try:
        destination = hydrate_archive(args.archive, args.destination)
        if args.command == "hydrate":
            return 0

        if not exec_command:
            raise RuntimeArchiveError("The run command requires a command after '--'")

        return subprocess.call(exec_command, cwd=destination, env=os.environ.copy())
    except RuntimeArchiveError as exc:
        print(f"runtime archive error: {exc}", file=sys.stderr)
        return 2


def _split_exec_command(argv: list[str]) -> tuple[list[str], list[str]]:
    if "--" not in argv:
        return argv, []

    separator = argv.index("--")
    return argv[:separator], argv[separator + 1 :]


if __name__ == "__main__":
    sys.exit(main())

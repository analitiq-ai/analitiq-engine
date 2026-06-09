from __future__ import annotations

import io
import sys
import tarfile
import urllib.error
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[3] / "src" / "runtime_archive.py"
SPEC = spec_from_file_location("runtime_archive_under_test", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
runtime_archive = module_from_spec(SPEC)
SPEC.loader.exec_module(runtime_archive)

RuntimeArchiveError = runtime_archive.RuntimeArchiveError
hydrate_archive = runtime_archive.hydrate_archive
main = runtime_archive.main


def _write_archive(archive_path: Path, source_dir: Path) -> None:
    with tarfile.open(archive_path, mode="w:gz") as archive:
        for path in sorted(source_dir.rglob("*")):
            archive.add(path, arcname=path.relative_to(source_dir))


def test_hydrate_archive_extracts_local_runtime_layout(tmp_path: Path) -> None:
    source = tmp_path / "source"
    pipeline_dir = source / "pipelines" / "pipeline-1"
    pipeline_dir.mkdir(parents=True)
    (source / "pipelines" / "manifest.json").write_text(
        '{"pipelines": []}', encoding="utf-8"
    )
    (pipeline_dir / "pipeline.json").write_text("{}", encoding="utf-8")

    archive_path = tmp_path / "runtime.tar.gz"
    _write_archive(archive_path, source)

    destination = tmp_path / "destination"

    hydrated = hydrate_archive(archive_path, destination)

    assert hydrated == destination.resolve()
    assert (destination / "pipelines" / "manifest.json").is_file()
    assert (destination / "pipelines" / "pipeline-1" / "pipeline.json").is_file()


def test_hydrate_archive_downloads_http_url(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    (source / "pipelines").mkdir(parents=True)
    (source / "pipelines" / "manifest.json").write_text(
        '{"pipelines": []}', encoding="utf-8"
    )
    archive_path = tmp_path / "runtime.tar.gz"
    _write_archive(archive_path, source)
    payload = archive_path.read_bytes()

    captured: dict[str, object] = {}

    def fake_urlopen(url, timeout=None):
        captured["url"] = url
        captured["timeout"] = timeout
        return io.BytesIO(payload)

    monkeypatch.setattr(runtime_archive.urllib.request, "urlopen", fake_urlopen)

    destination = tmp_path / "destination"
    hydrated = hydrate_archive("https://example.com/config.tgz", destination)

    assert captured["url"] == "https://example.com/config.tgz"
    assert hydrated == destination.resolve()
    assert (destination / "pipelines" / "manifest.json").is_file()


def test_hydrate_archive_wraps_download_failure(tmp_path: Path, monkeypatch) -> None:
    def fake_urlopen(url, timeout=None):
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(runtime_archive.urllib.request, "urlopen", fake_urlopen)

    try:
        hydrate_archive("https://example.com/missing.tgz", tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "download" in str(exc).lower()
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_requires_manifest(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "other.json").write_text("{}", encoding="utf-8")
    archive_path = tmp_path / "runtime.tar.gz"
    _write_archive(archive_path, source)

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "pipelines/manifest.json" in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_rejects_parent_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "runtime.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        payload = b"{}"
        info = tarfile.TarInfo("../escape.json")
        info.size = len(payload)
        archive.addfile(info, fileobj=io.BytesIO(payload))

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "parent traversal" in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_main_run_hydrates_then_executes_command(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "pipelines").mkdir(parents=True)
    (source / "pipelines" / "manifest.json").write_text(
        '{"pipelines": []}', encoding="utf-8"
    )
    archive_path = tmp_path / "runtime.tar.gz"
    _write_archive(archive_path, source)

    destination = tmp_path / "destination"
    marker = "ran.txt"

    exit_code = main(
        [
            "run",
            str(archive_path),
            "-C",
            str(destination),
            "--",
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({marker!r}).write_text('ok')",
        ]
    )

    assert exit_code == 0
    assert (destination / "pipelines" / "manifest.json").is_file()
    assert (destination / marker).read_text(encoding="utf-8") == "ok"

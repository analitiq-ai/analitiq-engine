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
    assert captured["timeout"] == runtime_archive._DOWNLOAD_TIMEOUT_SECONDS
    assert hydrated == destination.resolve()
    assert (destination / "pipelines" / "manifest.json").is_file()


# A presigned-style URL whose signature must never appear in any error message.
_SIGNED_URL = "https://bucket.s3.amazonaws.com/cfg.tgz?X-Amz-Signature=SECRETSIG"


def test_hydrate_archive_wraps_download_failure(tmp_path: Path, monkeypatch) -> None:
    def fake_urlopen(url, timeout=None):
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(runtime_archive.urllib.request, "urlopen", fake_urlopen)

    try:
        hydrate_archive(_SIGNED_URL, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "download" in str(exc).lower()
        assert "SECRETSIG" not in str(exc)
        assert "bucket.s3.amazonaws.com/cfg.tgz" in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_wraps_http_error(tmp_path: Path, monkeypatch) -> None:
    def fake_urlopen(url, timeout=None):
        raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)

    monkeypatch.setattr(runtime_archive.urllib.request, "urlopen", fake_urlopen)

    try:
        hydrate_archive(_SIGNED_URL, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "403" in str(exc)
        assert "SECRETSIG" not in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_rejects_empty_download(tmp_path: Path, monkeypatch) -> None:
    def fake_urlopen(url, timeout=None):
        return io.BytesIO(b"")

    monkeypatch.setattr(runtime_archive.urllib.request, "urlopen", fake_urlopen)

    try:
        hydrate_archive(_SIGNED_URL, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "empty" in str(exc).lower()
        assert "SECRETSIG" not in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_cleans_up_temp_file_on_download_failure(
    tmp_path: Path, monkeypatch
) -> None:
    created: list[str] = []
    real_mkstemp = runtime_archive.tempfile.mkstemp

    def tracking_mkstemp(*args, **kwargs):
        handle, name = real_mkstemp(*args, **kwargs)
        created.append(name)
        return handle, name

    def fake_urlopen(url, timeout=None):
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(runtime_archive.tempfile, "mkstemp", tracking_mkstemp)
    monkeypatch.setattr(runtime_archive.urllib.request, "urlopen", fake_urlopen)

    try:
        hydrate_archive("https://example.com/missing.tgz", tmp_path / "destination")
    except RuntimeArchiveError:
        pass
    else:
        raise AssertionError("Expected RuntimeArchiveError")

    assert created, "expected a temp file to be created"
    assert not Path(created[0]).exists(), "temp file should be cleaned up on failure"


def test_is_remote_url_scheme_boundary() -> None:
    assert runtime_archive._is_remote_url("http://example.com/x.tgz") is True
    assert runtime_archive._is_remote_url("https://example.com/x.tgz") is True
    # urlparse lowercases the scheme, so case does not matter.
    assert runtime_archive._is_remote_url("HTTPS://example.com/x.tgz") is True
    # Local-resource schemes must never be treated as remote: this is the guard
    # the download path relies on to refuse file://, s3://, ftp://.
    assert runtime_archive._is_remote_url("file:///etc/passwd") is False
    assert runtime_archive._is_remote_url("s3://bucket/key") is False
    assert runtime_archive._is_remote_url("ftp://host/x") is False
    assert runtime_archive._is_remote_url("/local/path/config.tgz") is False
    assert runtime_archive._is_remote_url(Path("/local/path/config.tgz")) is False


def test_hydrate_archive_local_path_not_found(tmp_path: Path) -> None:
    try:
        hydrate_archive(tmp_path / "missing.tar", tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "not found" in str(exc).lower()
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_requires_manifest(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "pipelines").mkdir(parents=True)
    (source / "pipelines" / "other.json").write_text("{}", encoding="utf-8")
    archive_path = tmp_path / "runtime.tar.gz"
    _write_archive(archive_path, source)

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "pipelines/manifest.json" in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_rejects_path_outside_runtime_dirs(tmp_path: Path) -> None:
    # A bundle must not be able to overwrite engine code (e.g. src/main.py)
    # when it hydrates into the engine working directory.
    archive_path = tmp_path / "runtime.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        payload = b"print('pwned')"
        info = tarfile.TarInfo("src/main.py")
        info.size = len(payload)
        archive.addfile(info, fileobj=io.BytesIO(payload))

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "allowed runtime directories" in str(exc)
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


def test_hydrate_archive_rejects_absolute_path(tmp_path: Path) -> None:
    archive_path = tmp_path / "runtime.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        payload = b"{}"
        info = tarfile.TarInfo("/etc/evil.json")
        info.size = len(payload)
        archive.addfile(info, fileobj=io.BytesIO(payload))

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "absolute path" in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_rejects_symlink(tmp_path: Path) -> None:
    archive_path = tmp_path / "runtime.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        info = tarfile.TarInfo("link.json")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        archive.addfile(info)

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "unsupported link" in str(exc)
    else:
        raise AssertionError("Expected RuntimeArchiveError")


def test_hydrate_archive_rejects_hardlink(tmp_path: Path) -> None:
    archive_path = tmp_path / "runtime.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        info = tarfile.TarInfo("hard.json")
        info.type = tarfile.LNKTYPE
        info.linkname = "manifest.json"
        archive.addfile(info)

    try:
        hydrate_archive(archive_path, tmp_path / "destination")
    except RuntimeArchiveError as exc:
        assert "unsupported link" in str(exc)
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


def test_main_hydrate_returns_zero_on_success(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "pipelines").mkdir(parents=True)
    (source / "pipelines" / "manifest.json").write_text(
        '{"pipelines": []}', encoding="utf-8"
    )
    archive_path = tmp_path / "runtime.tar.gz"
    _write_archive(archive_path, source)
    destination = tmp_path / "destination"

    exit_code = main(["hydrate", str(archive_path), "-C", str(destination)])

    assert exit_code == 0
    assert (destination / "pipelines" / "manifest.json").is_file()


def test_main_hydrate_returns_error_code_on_failure(tmp_path: Path) -> None:
    # The Docker entrypoint relies on this non-zero exit to abort before the
    # engine starts; a bad archive must never return 0.
    missing = tmp_path / "missing.tar.gz"
    destination = tmp_path / "destination"

    exit_code = main(["hydrate", str(missing), "-C", str(destination)])

    assert exit_code == 2

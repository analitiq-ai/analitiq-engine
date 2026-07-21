"""Unit tests for LocalFileStorage."""

from unittest.mock import patch

import pytest

from src.destination.storage.local import LocalFileStorage


@pytest.fixture
def storage():
    return LocalFileStorage()


@pytest.fixture
async def connected_storage(tmp_path):
    s = LocalFileStorage()
    await s.connect({"path": str(tmp_path), "create_dirs": True})
    return s, tmp_path


class TestConnect:
    async def test_raises_when_path_missing(self, storage, tmp_path):
        with pytest.raises(ValueError, match="requires 'path'"):
            await storage.connect({})

    async def test_raises_when_path_falsy(self, storage):
        with pytest.raises(ValueError, match="requires 'path'"):
            await storage.connect({"path": ""})

    async def test_create_dirs_false_does_not_mkdir(self, storage, tmp_path):
        target = tmp_path / "nonexistent"
        await storage.connect({"path": str(target), "create_dirs": False})
        assert not target.exists()
        assert storage._connected

    async def test_reconnect_replaces_base_path(self, storage, tmp_path):
        path1 = tmp_path / "p1"
        path2 = tmp_path / "p2"
        await storage.connect({"path": str(path1)})
        await storage.connect({"path": str(path2)})
        assert storage._base_path == path2


class TestRequirePath:
    def test_raises_when_not_connected(self, storage):
        with pytest.raises(IOError, match="Storage not connected"):
            storage._require_path("file.txt")

    def test_raises_when_connected_but_base_path_none(self, storage):
        storage._connected = True
        storage._base_path = None
        with pytest.raises(IOError, match="Storage not connected"):
            storage._require_path("file.txt")

    async def test_returns_base_path_joined(self, connected_storage):
        s, base = connected_storage
        result = s._require_path("subdir/file.txt")
        assert result == base / "subdir" / "file.txt"


class TestWriteFile:
    async def test_raises_when_not_connected(self, storage):
        with pytest.raises(IOError, match="Storage not connected"):
            await storage.write_file("file.txt", b"data")

    async def test_writes_file_and_returns_path(self, connected_storage):
        s, base = connected_storage
        result = await s.write_file("out.txt", b"hello")
        assert result == str(base / "out.txt")
        assert (base / "out.txt").read_bytes() == b"hello"

    async def test_overwrites_existing_file(self, connected_storage):
        s, base = connected_storage
        (base / "f.txt").write_bytes(b"old")
        await s.write_file("f.txt", b"new")
        assert (base / "f.txt").read_bytes() == b"new"

    async def test_creates_parent_dirs(self, connected_storage):
        s, base = connected_storage
        await s.write_file("a/b/c.txt", b"nested")
        assert (base / "a" / "b" / "c.txt").read_bytes() == b"nested"

    @staticmethod
    async def test_no_tmp_file_left_after_write(connected_storage):
        s, base = connected_storage
        await s.write_file("out.jsonl", b"data")
        assert [p.name for p in base.iterdir()] == ["out.jsonl"]

    @staticmethod
    async def test_failed_write_leaves_committed_file_intact(connected_storage):
        """The write is atomic (temp + rename, issue #306): a failure
        mid-write must not truncate a previously committed file at the
        final path, and must not leave the temp file behind."""
        s, base = connected_storage
        (base / "f.jsonl").write_bytes(b"committed")

        with (
            patch(
                "src.destination.storage.local.aiofiles.open",
                side_effect=OSError(5, "boom"),
            ),
            pytest.raises(OSError, match="boom"),
        ):
            await s.write_file("f.jsonl", b"replacement")

        assert (base / "f.jsonl").read_bytes() == b"committed"
        assert not (base / "f.jsonl.tmp").exists()


class TestFileExists:
    async def test_returns_false_when_not_connected(self, storage):
        result = await storage.file_exists("any.txt")
        assert result is False

    async def test_returns_false_when_base_path_none(self, storage):
        storage._connected = True
        storage._base_path = None
        assert await storage.file_exists("any.txt") is False

    async def test_returns_false_when_file_absent(self, connected_storage):
        s, _ = connected_storage
        assert await s.file_exists("missing.txt") is False

    async def test_returns_true_when_file_present(self, connected_storage):
        s, base = connected_storage
        (base / "exists.txt").write_bytes(b"")
        assert await s.file_exists("exists.txt") is True


class TestReadFile:
    async def test_raises_when_not_connected(self, storage):
        with pytest.raises(IOError, match="Storage not connected"):
            await storage.read_file("file.txt")

    async def test_raises_when_file_absent(self, connected_storage):
        s, _ = connected_storage
        with pytest.raises(FileNotFoundError):
            await s.read_file("ghost.txt")

    async def test_reads_file_contents(self, connected_storage):
        s, base = connected_storage
        (base / "data.txt").write_bytes(b"content")
        result = await s.read_file("data.txt")
        assert result == b"content"


class TestDeleteFile:
    async def test_raises_when_not_connected(self, storage):
        with pytest.raises(IOError, match="Storage not connected"):
            await storage.delete_file("file.txt")

    async def test_returns_false_when_file_absent(self, connected_storage):
        s, _ = connected_storage
        result = await s.delete_file("nonexistent.txt")
        assert result is False

    async def test_deletes_existing_file_and_returns_true(self, connected_storage):
        s, base = connected_storage
        (base / "to_delete.txt").write_bytes(b"bye")
        result = await s.delete_file("to_delete.txt")
        assert result is True
        assert not (base / "to_delete.txt").exists()


class TestHealthCheck:
    async def test_returns_false_when_not_connected(self, storage):
        assert await storage.health_check() is False

    async def test_returns_true_when_connected_and_writable(self, connected_storage):
        s, _ = connected_storage
        assert await s.health_check() is True

    async def test_returns_false_after_disconnect(self, connected_storage):
        s, _ = connected_storage
        await s.disconnect()
        assert await s.health_check() is False

    async def test_returns_false_when_base_path_is_file(self, storage, tmp_path):
        f = tmp_path / "notadir.txt"
        f.write_bytes(b"x")
        storage._base_path = f
        storage._connected = True
        assert await storage.health_check() is False


class TestDisconnect:
    async def test_idempotent(self, connected_storage):
        s, _ = connected_storage
        await s.disconnect()
        await s.disconnect()
        assert not s._connected

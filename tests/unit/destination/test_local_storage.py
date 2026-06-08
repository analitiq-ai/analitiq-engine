"""Unit tests for LocalFileStorage."""

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


class TestRequirePath:
    def test_raises_when_not_connected(self, storage):
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

    async def test_creates_parent_dirs(self, connected_storage):
        s, base = connected_storage
        await s.write_file("a/b/c.txt", b"nested")
        assert (base / "a" / "b" / "c.txt").read_bytes() == b"nested"


class TestAppendToFile:
    async def test_raises_when_not_connected(self, storage):
        with pytest.raises(IOError, match="Storage not connected"):
            await storage.append_to_file("file.txt", b"data")

    async def test_appends_data_and_returns_length(self, connected_storage):
        s, base = connected_storage
        (base / "f.txt").write_bytes(b"first")
        n = await s.append_to_file("f.txt", b"_second")
        assert n == len(b"_second")
        assert (base / "f.txt").read_bytes() == b"first_second"


class TestFileExists:
    async def test_returns_false_when_not_connected(self, storage):
        result = await storage.file_exists("any.txt")
        assert result is False

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

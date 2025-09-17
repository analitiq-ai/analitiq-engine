class aioresponses:  # pragma: no cover - simple stub for tests
    """Minimal stub of :mod:`aioresponses` used by test fixtures."""

    def __init__(self, *args, **kwargs):
        self.calls = []

    def __enter__(self):  # noqa: D401 - context manager protocol only
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    # Allow the stub to be called as a function to register fake responses.
    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self

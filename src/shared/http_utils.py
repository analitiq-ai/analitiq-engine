"""Shared HTTP utilities."""


def join_url(base: str, path: str) -> str:
    # urljoin treats a leading '/' on path as absolute-path-relative and
    # drops base's path segment (e.g. /api/v1 + /Foo -> /Foo, not /api/v1/Foo).
    return base.rstrip("/") + "/" + path.lstrip("/")

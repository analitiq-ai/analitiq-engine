"""Regenerate the committed consumption manifest.

Run as ``python -m tools.consumption_manifest``.
"""

from .manifest import CONSUMPTION_MANIFEST_PATH, render_consumption_manifest

CONSUMPTION_MANIFEST_PATH.write_text(render_consumption_manifest())
print(f"wrote {CONSUMPTION_MANIFEST_PATH}")

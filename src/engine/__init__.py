"""Engine package.

Concrete entry points live in submodules (``engine.engine``,
``engine.pipeline``, ``engine.type_map``, …). This init stays empty so
cross-cutting sub-packages like ``engine.type_map`` can be imported by
``shared`` without dragging ``StreamingEngine`` through the cycle.
"""

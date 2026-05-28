"""Spin up DBs, seed, run engine, assert, tear down.

Importable from pytest or runnable as ``python -m tests.e2e_databases.orchestrator``.

The orchestrator is responsible for the surrounding plumbing: starting the
container fixtures, writing the per-test configs, running the engine via
compose, verifying the destination, and cleaning up. The actual write path
(extract → cast → load) lives in the engine itself — this module is its
test harness, not a re-implementation of it.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, fields
from typing import List, Optional, get_args

from tests.e2e_databases.databases import all_specs, spec_for
from tests.e2e_databases.databases._base import DatabaseSpec
from tests.e2e_databases.databases._docker import (
    compose_down,
    compose_down_all,
    compose_recreate,
    compose_run_source_engine,
)
from tests.e2e_databases.factory import (
    GeneratedPair,
    ReplicationMode,
    WriteMode,
    build_pair,
)
from tests.e2e_databases.seeds import SeedRow, canonical_seed_rows

logger = logging.getLogger(__name__)


class PipelineRunFailed(RuntimeError):
    """The source_engine container exited non-zero."""


class DestinationMismatch(AssertionError):
    """Destination rows did not match the canonical seed."""


@dataclass(frozen=True)
class PipelineResult:
    pair: GeneratedPair
    actual_rows: List[SeedRow]


class E2ETestRun:
    """One source → destination pair × replication mode."""

    def __init__(
        self,
        source: DatabaseSpec,
        destination: DatabaseSpec,
        mode: ReplicationMode,
        write_mode: WriteMode = "insert",
    ) -> None:
        self.source = source
        self.destination = destination
        self.mode = mode
        self.write_mode = write_mode
        self._pair: Optional[GeneratedPair] = None
        self._engine_services_started = False

    def setup(self) -> GeneratedPair:
        """Bring the DBs up, seed the source, prepare the destination, and
        write the pipeline config.

        ``build_pair`` wipes runtime state, so the first ``sync()`` after this
        starts from an empty cursor bookmark.
        """
        logger.info(
            "E2E setup: %s -> %s (%s, %s)",
            self.source.slug,
            self.destination.slug,
            self.mode,
            self.write_mode,
        )
        self._bring_up_dbs()
        self._seed()
        self._pair = build_pair(
            self.source, self.destination, self.mode, self.write_mode
        )
        return self._pair

    def sync(self) -> List[SeedRow]:
        """Invoke the engine once and read the destination back.

        Runtime state is preserved between calls, so a second ``sync()``
        resumes from the cursor bookmark the previous one left behind. That
        resumption is what makes the incremental delta and no-op assertions
        meaningful — without it every sync would re-read the whole source.
        """
        if self._pair is None:
            raise RuntimeError("sync() requires setup() first")
        self._invoke_engine(self._pair.pipeline_id)
        return self.destination.read_destination()

    def run(self) -> PipelineResult:
        """Single full sync: set up, sync once, assert the destination matches
        the canonical seed."""
        self.setup()
        actual = self.sync()
        self._assert_rows(actual, canonical_seed_rows())
        assert self._pair is not None  # set by setup()
        return PipelineResult(pair=self._pair, actual_rows=actual)

    def teardown(self, *, keep_databases: bool = False) -> None:
        """Bring down what this run brought up.

        The engine services are always torn down: the destination service
        binds one ``PIPELINE_ID`` at start, so the next run must get a fresh
        one. DB containers are optionally kept up (``keep_databases=True``)
        so a parameterized matrix can reuse them across pairs — every run
        re-seeds the source and drops the destination table, so reused
        containers still start each run from a clean table.
        """
        try:
            if self._engine_services_started:
                compose_down("source_engine", "engine-destination")
        finally:
            if not keep_databases:
                self.source.down("source")
                # Same DB type still means two distinct containers (source +
                # destination roles), so always bring the destination down too.
                self.destination.down("destination")

    # ---- helpers --------------------------------------------------------

    def _bring_up_dbs(self) -> None:
        self.source.up("source")
        self.destination.up("destination")

    def _seed(self) -> None:
        self.source.seed("source", canonical_seed_rows())
        self.destination.prepare_destination()

    def _invoke_engine(self, pipeline_id: str) -> None:
        os.environ["PIPELINE_ID"] = pipeline_id
        self._engine_services_started = True
        # Force-recreate the destination so it binds THIS pipeline's config.
        # source_engine depends on it being healthy, so compose run would
        # otherwise reuse whatever destination happens to be running.
        compose_recreate("engine-destination")
        result = compose_run_source_engine(pipeline_id)
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if stdout:
            logger.info("engine stdout:\n%s", stdout)
        if result.returncode != 0:
            # The engine logs its stack trace to stdout; docker compose's
            # container-lifecycle chatter is what lands on stderr. Fold the
            # stdout tail into the exception so the actual failure reason
            # travels with it instead of a bare exit code in the pytest report.
            logger.error("engine stderr:\n%s", stderr)
            tail = "\n".join(stdout.splitlines()[-30:])
            raise PipelineRunFailed(
                f"source_engine exited with code {result.returncode} for "
                f"pipeline {pipeline_id}\n--- engine output (tail) ---\n{tail}"
            )
        if stderr:
            logger.info("engine stderr:\n%s", stderr)

    @staticmethod
    def _assert_rows(actual: List[SeedRow], expected: List[SeedRow]) -> None:
        if len(actual) != len(expected):
            raise DestinationMismatch(
                f"row count mismatch: expected {len(expected)}, got {len(actual)}"
            )
        for got, want in zip(actual, expected):
            if got == want:
                continue
            diffs = [
                f"{f.name}: expected {getattr(want, f.name)!r}, "
                f"got {getattr(got, f.name)!r}"
                for f in fields(want)
                if getattr(got, f.name) != getattr(want, f.name)
            ]
            raise DestinationMismatch(f"row id={want.id} mismatch: " + "; ".join(diffs))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _list_specs() -> None:
    specs = all_specs()
    width = max(len(slug) for slug in specs) + 2
    for slug, spec in sorted(specs.items()):
        reason = spec.available
        status = "ready" if reason is None else f"skipped ({reason})"
        cloud = " [cloud]" if spec.is_cloud else ""
        print(f"  {slug:<{width}} {status}{cloud}")


def _main() -> int:
    logging.basicConfig(
        level=os.environ.get("E2E_LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(prog="e2e-databases")
    parser.add_argument(
        "--list", action="store_true", help="List known DBs and skip reasons"
    )
    parser.add_argument(
        "--down-all", action="store_true", help="Tear down every framework container"
    )
    parser.add_argument("--source", help="Source DB slug")
    parser.add_argument("--dest", help="Destination DB slug")
    parser.add_argument(
        "--mode", choices=get_args(ReplicationMode), default="full_refresh"
    )
    parser.add_argument(
        "--write-mode", choices=get_args(WriteMode), default="insert"
    )
    parser.add_argument(
        "--keep-up",
        action="store_true",
        help="Skip teardown on success — useful for debugging",
    )
    args = parser.parse_args()

    if args.list:
        _list_specs()
        return 0

    if args.down_all:
        compose_down_all()
        return 0

    if not (args.source and args.dest):
        parser.error("--source and --dest are required (or pass --list / --down-all)")

    src = spec_for(args.source)
    dst = spec_for(args.dest)
    for spec in (src, dst):
        reason = spec.available
        if reason is not None:
            print(f"cannot run: {spec.slug} unavailable — {reason}", file=sys.stderr)
            return 2

    run = E2ETestRun(src, dst, args.mode, args.write_mode)
    try:
        result = run.run()
        print(
            f"OK: {len(result.actual_rows)} rows transferred via pipeline {result.pair.pipeline_id}"
        )
        return 0
    except (PipelineRunFailed, DestinationMismatch) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        if not args.keep_up:
            run.teardown()


if __name__ == "__main__":
    sys.exit(_main())

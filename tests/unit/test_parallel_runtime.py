"""Regression checks for interrupted runs and worker snapshots under tests/staging."""

import json
from pathlib import Path
from unittest.mock import Mock

import parallel_runtime
from pytest import MonkeyPatch


def test_finish_cancelled_run_only_cleans_its_own_workers(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    cleanup = Mock(return_value=[])
    monkeypatch.setattr(parallel_runtime, "cleanup_worker", cleanup)
    run_id = "a" * 32
    directory = Path("tests/staging/parallel") / run_id
    (directory / "worker-1").mkdir(parents=True)
    (directory / "results").mkdir()
    neighbor = directory.parent / ("b" * 32) / "worker-1"
    neighbor.mkdir(parents=True)
    summary = directory / "results/summary.json"
    summary.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": "running",
                "errors": [],
                "workers": {},
                "features": {
                    "one": {"status": "running"},
                    "two": {"status": "not_run"},
                    "three": {"status": "passed"},
                },
            }
        )
    )

    parallel_runtime.finish_cancelled_run(run_id)
    parallel_runtime.finish_cancelled_run(run_id)

    cleanup.assert_called_once_with(
        directory / "worker-1", f"cht-{run_id}-w1", summary.parent, None
    )
    assert neighbor.exists()
    report = json.loads(summary.read_text())
    assert report["status"] == "cancelled"
    assert report["features"] == {
        "one": {"status": "cancelled"},
        "two": {"status": "not_run", "not_run_reason": "cancelled"},
        "three": {"status": "passed"},
    }


def test_snapshot_excludes_previous_sessions_and_its_own_destination(
    tmp_path: Path,
) -> None:
    for directory in (
        "ch_tools",
        "resources",
        "dist",
        "tests/staging",
        "tests/reports",
    ):
        (tmp_path / directory).mkdir(parents=True)
    for filename in (
        ".python-version",
        "Makefile",
        "README.md",
        "pyproject.toml",
        "uv.lock",
    ):
        (tmp_path / filename).touch()
    (tmp_path / "ch_tools/version.txt").write_text("1.0")
    (tmp_path / "dist/clickhouse_tools-1.0-py3-none-any.whl").touch()
    (tmp_path / "tests/.session_conf.sav").touch()
    (tmp_path / "tests/test_input").write_text("uncommitted input")
    destination = tmp_path / "tests/staging/parallel/example/source"

    parallel_runtime.snapshot(tmp_path, destination)

    assert list((destination / "tests").iterdir()) == [destination / "tests/test_input"]
    assert (destination / "tests/test_input").read_text() == "uncommitted input"

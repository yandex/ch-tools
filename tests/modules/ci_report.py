"""
Behave formatter for CI: a line per scenario and a summary of the run.

The full output of behave goes elsewhere, so the log of a job shows only what ran and how.
On GitHub Actions failures become annotations and the summary goes to the page of the job.
"""

import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from behave.formatter.base import Formatter
from behave.runner_util import collect_feature_locations

STATUSES = ("passed", "failed", "skipped")


@dataclass
class FeatureResult:
    """Scenario counts and wall time of a feature."""

    name: str
    counts: dict = field(default_factory=lambda: dict.fromkeys(STATUSES, 0))
    seconds: float = 0.0


@dataclass
class Failure:
    """The first failed step of a scenario."""

    feature: str
    scenario: str
    step: str
    location: str
    line: int
    error: str


class CIReport(Formatter):
    """One line per scenario, failures with their step and a summary at the end."""

    name = "ci_report"
    description = "A line per scenario and a summary for CI."

    def __init__(self, stream_opener: Any, config: Any) -> None:
        super().__init__(stream_opener, config)
        self.planned = [
            os.path.basename(location.filename)
            for location in collect_feature_locations(config.paths)
            if not config.exclude(location.filename)
        ]
        self.features: list[FeatureResult] = []
        self.failures: list[Failure] = []
        self.scenario_: Any = None
        self.scenario_started = 0.0
        self.feature_started = 0.0
        self.run_started = time.monotonic()
        self.github = os.getenv("GITHUB_ACTIONS") == "true"

    def _write(self, text: str) -> None:
        stream = self.open()
        stream.write(text + "\n")
        stream.flush()

    def uri(self, uri: str) -> None:
        self.features.append(FeatureResult(os.path.basename(uri)))
        self.feature_started = time.monotonic()
        self._write(
            f"{self.features[-1].name} [{len(self.features)}/{len(self.planned)}]"
        )

    def scenario(self, scenario: Any) -> None:
        self._finish_scenario()
        self.scenario_ = scenario
        self.scenario_started = time.monotonic()

    def eof(self) -> None:
        self._finish_scenario()
        if self.features:
            self.features[-1].seconds = time.monotonic() - self.feature_started

    def _finish_scenario(self) -> None:
        scenario, self.scenario_ = self.scenario_, None
        if scenario is None:
            return
        status = scenario.status.name
        status = status if status in STATUSES else "skipped"
        if scenario.hook_failed or any(
            step.status.name == "undefined" for step in scenario.all_steps
        ):
            status = "failed"
        feature = self.features[-1]
        feature.counts[status] += 1
        label = "FAILED" if status == "failed" else status
        seconds = time.monotonic() - self.scenario_started
        self._write(f"  {label:<7} {_duration(seconds):>6}  {scenario.name.strip()}")
        if status == "failed":
            self._report_failure(feature.name, scenario)

    def _report_failure(self, feature: str, scenario: Any) -> None:
        step = next(
            (s for s in scenario.all_steps if s.status.name in ("failed", "undefined")),
            None,
        )
        if step is None:
            failure = Failure(
                feature,
                scenario.name.strip(),
                "a hook of the scenario",
                str(scenario.location.filename),
                scenario.location.line,
                "see the full log",
            )
        else:
            error = step.exception
            failure = Failure(
                feature,
                scenario.name.strip(),
                f"{step.keyword} {step.name}",
                str(step.location.filename),
                step.location.line,
                (
                    " ".join(f"{type(error).__name__}: {error}".split())[:300]
                    if error
                    else f"{step.status.name} step"
                ),
            )
        self.failures.append(failure)
        indent = " " * 18
        self._write(f"{indent}{failure.step} ({failure.location}:{failure.line})")
        self._write(f"{indent}{failure.error}")
        if self.github:
            message = _escape(f"{failure.scenario}: {failure.step}\n{failure.error}")
            self._write(
                f"::error file=tests/{failure.location},line={failure.line}"
                f",title={_escape(feature, prop=True)}::{message}"
            )

    def close(self) -> None:
        self._finish_scenario()
        totals = dict.fromkeys(STATUSES, 0)
        for feature in self.features:
            for status, count in feature.counts.items():
                totals[status] += count
        failed = bool(totals["failed"]) or not self.features
        not_run = self.planned[len(self.features) :]
        result = "failed" if failed else "passed"
        counts = ", ".join(f"{totals[status]} {status}" for status in STATUSES)
        elapsed = _duration(time.monotonic() - self.run_started)
        self._write(f"\nIntegration tests {result}: {counts} in {elapsed}.")
        if not_run:
            self._write(f"Did not run: {', '.join(not_run)}.")
        summary = os.getenv("GITHUB_STEP_SUMMARY")
        if self.github and summary:
            with open(summary, "a", encoding="utf-8") as out:
                out.write(self._markdown(result, counts, elapsed, not_run))
        self.close_stream()

    def _markdown(
        self, result: str, counts: str, elapsed: str, not_run: list[str]
    ) -> str:
        icon = {"passed": "✅", "failed": "❌"}[result]
        lines = [f"## {icon} Integration tests {result}", "", f"{counts} in {elapsed}."]
        if self.failures:
            lines += ["", "### Failures", ""]
            lines += [
                f"- **{_cell(f.feature)}** · {_cell(f.scenario)}: `{_cell(f.step)}` "
                f"(`tests/{f.location}:{f.line}`)<br>`{_cell(f.error)}`"
                for f in self.failures
            ]
        lines += [
            "",
            "### Features",
            "",
            "| | Feature | Passed | Failed | Skipped | Time |",
            "|---|---|--:|--:|--:|--:|",
        ]
        for feature in self.features:
            mark = "❌" if feature.counts["failed"] else "✅"
            lines.append(
                f"| {mark} | {_cell(feature.name)} | {feature.counts['passed']} "
                f"| {feature.counts['failed']} | {feature.counts['skipped']} "
                f"| {_duration(feature.seconds)} |"
            )
        lines += [f"| ⏸️ | {_cell(name)} | | | | did not run |" for name in not_run]
        lines += [
            "",
            "The full log, the JUnit reports and the logs of the containers are in the "
            "artifacts of the run.",
            "",
        ]
        return "\n".join(lines)


def _duration(seconds: float) -> str:
    minutes, seconds = divmod(int(seconds), 60)
    return f"{minutes}m{seconds:02d}s" if minutes else f"{seconds}s"


def _escape(text: str, *, prop: bool = False) -> str:
    """Workflow command values must not end the command early."""
    text = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return text.replace(":", "%3A").replace(",", "%2C") if prop else text


def _cell(text: Optional[str]) -> str:
    return (text or "").replace("|", "\\|").replace("`", "'").replace("\n", " ")

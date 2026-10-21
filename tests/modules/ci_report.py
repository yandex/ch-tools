"""
Behave formatter for CI: a line per scenario and a summary of the run.

The full output of behave goes elsewhere, so the log of a job shows only what ran and how.
On GitHub Actions failures become annotations and the summary goes to the page of the job.
"""

import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from behave.formatter.base import Formatter
from behave.runner_util import collect_feature_locations

# Behave did not get to untested scenarios.
STATUSES = {
    "passed": "passed",
    "failed": "failed",
    "skipped": "skipped",
    "untested": "not run",
}


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

    def _write_untrusted(self, text: str) -> None:
        """Text of tests: on GitHub Actions its lines cannot run workflow commands."""
        token = uuid.uuid4().hex
        if self.github:
            self._write(f"::stop-commands::{token}")
        self._write(text)
        if self.github:
            self._write(f"::{token}::")

    def uri(self, uri: str) -> None:
        self.features.append(FeatureResult(_line(os.path.basename(uri))))
        self.feature_started = time.monotonic()
        self._write(
            f"[{len(self.features)}/{len(self.planned)}] {self.features[-1].name}"
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
        label = "FAILED" if status == "failed" else STATUSES[status]
        seconds = time.monotonic() - self.scenario_started
        self._write(f"  {label:<7} {_duration(seconds):>6}  {_line(scenario.name)}")
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
                _line(scenario.name),
                "a hook of the scenario",
                _line(str(scenario.location.filename)),
                int(scenario.location.line),
                "see the full log",
            )
        else:
            error = step.exception
            failure = Failure(
                feature,
                _line(scenario.name),
                _line(f"{step.keyword} {step.name}"),
                _line(str(step.location.filename)),
                int(step.location.line),
                (
                    _line(f"{type(error).__name__}: {error}")[:300]
                    if error
                    else f"{step.status.name} step"
                ),
            )
        self.failures.append(failure)
        indent = " " * 18
        self._write_untrusted(
            f"{indent}{failure.step} ({failure.location}:{failure.line})\n"
            f"{indent}{failure.error}"
        )
        if self.github:
            message = _escape(f"{failure.scenario}: {failure.step}\n{failure.error}")
            self._write(
                f"::error file={_escape(f'tests/{failure.location}', prop=True)}"
                f",line={failure.line},title={_escape(feature, prop=True)}::{message}"
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
        counts = ", ".join(f"{totals[s]} {name}" for s, name in STATUSES.items())
        elapsed = _duration(time.monotonic() - self.run_started)
        self._write(f"\nIntegration tests {result}: {counts} in {elapsed}.")
        if not_run:
            self._write(f"Did not run: {', '.join(_line(n) for n in not_run)}.")
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
                f"- **{_text(f.feature)}** · {_text(f.scenario)}: `{_code(f.step)}` "
                f"(`tests/{_code(f.location)}:{f.line}`)<br>`{_code(f.error)}`"
                for f in self.failures
            ]
        lines += [
            "",
            "### Features",
            "",
            "| | Feature | Passed | Failed | Skipped | Not run | Time |",
            "|---|---|--:|--:|--:|--:|--:|",
        ]
        for feature in self.features:
            mark = "❌" if feature.counts["failed"] else "✅"
            counts = " | ".join(str(feature.counts[status]) for status in STATUSES)
            lines.append(
                f"| {mark} | {_text(feature.name)} | {counts} "
                f"| {_duration(feature.seconds)} |"
            )
        lines += [f"| ⏸️ | {_text(name)} | | | | | did not run |" for name in not_run]
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


def _line(text: str) -> str:
    """Names of tests on one line: the next line of a log could be a workflow command."""
    return " ".join(text.split())


def _text(text: str) -> str:
    """Text of tests renders as is: no links, images, HTML or cells of its own."""
    return re.sub(r"([\\`*_{}\[\]()<>#+\-.!|~@:&])", r"\\\1", _line(text))


def _code(text: str) -> str:
    """Text of tests in a code span: it cannot close the span or the cell."""
    return _line(text).replace("`", "'").replace("|", "\\|")

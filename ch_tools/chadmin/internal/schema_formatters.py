"""
Formatters for displaying differences between ClickHouse table schemas.

Provides base class and concrete formatter implementations
for displaying differences in various styles (unified, side-by-side, normal).
"""

import re
from abc import ABC, abstractmethod
from difflib import SequenceMatcher, unified_diff
from typing import List, Tuple

from termcolor import colored

# Constants
DEFAULT_SIDE_BY_SIDE_WIDTH = 160


class BaseSchemaFormatter(ABC):
    """
    Base class for schema formatters.

    Provides common functionality for all formatters,
    including character-level difference highlighting and color application.
    """

    def __init__(self, colored_output: bool = True):
        """
        Args:
            colored_output: Whether to use colored output
        """
        self.colored_output = colored_output

    def format_line_with_color(self, line: str, color: str, bold: bool = False) -> str:
        """
        Apply color to a line.

        Args:
            line: Line to format
            color: Color (red, green, cyan, etc.)
            bold: Whether to use bold font

        Returns:
            Formatted line (with color if colored_output=True)
        """
        if not self.colored_output:
            return line

        attrs = ["bold"] if bold else []
        return colored(line, color, attrs=attrs)

    def highlight_line_differences(
        self, line1: str, line2: str
    ) -> Tuple[str, str, str]:
        """
        Highlight differences between two lines at character level.

        Args:
            line1: First line
            line2: Second line

        Returns:
            Tuple (highlighted_line1, highlighted_line2, marker_line)
            where marker_line contains "^" characters under differing characters
        """
        if line1 == line2:
            return line1, line2, ""

        matcher = SequenceMatcher(None, line1, line2)

        result1 = []
        result2 = []
        markers = []

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                # Equal parts - base color without bold
                text1 = line1[i1:i2]
                text2 = line2[j1:j2]
                if self.colored_output:
                    result1.append(colored(text1, "red"))
                    result2.append(colored(text2, "green"))
                else:
                    result1.append(text1)
                    result2.append(text2)
                markers.append(" " * (i2 - i1))
            elif tag == "replace":
                # Replaced text - bold on colored background
                text1 = line1[i1:i2]
                text2 = line2[j1:j2]
                if self.colored_output:
                    result1.append(colored(text1, "red", attrs=["bold"]))
                    result2.append(colored(text2, "green", attrs=["bold"]))
                else:
                    result1.append(text1)
                    result2.append(text2)
                # Markers for the longer segment
                max_len = max(i2 - i1, j2 - j1)
                markers.append("^" * max_len)
            elif tag == "delete":
                # Text only in line1 - bold red
                text1 = line1[i1:i2]
                if self.colored_output:
                    result1.append(colored(text1, "red", attrs=["bold"]))
                else:
                    result1.append(text1)
                markers.append("^" * (i2 - i1))
            elif tag == "insert":
                # Text only in line2 - bold green
                text2 = line2[j1:j2]
                if self.colored_output:
                    result2.append(colored(text2, "green", attrs=["bold"]))
                else:
                    result2.append(text2)
                # For insertions, need to add padding in result1 and markers
                pad_len = j2 - j1
                result1.append(" " * pad_len)
                markers.append("^" * pad_len)

        marker_line = "".join(markers).rstrip()

        return "".join(result1), "".join(result2), marker_line

    @abstractmethod
    def format(
        self,
        schema1_lines: List[str],
        schema2_lines: List[str],
        name1: str,
        name2: str,
        **kwargs: int,
    ) -> str:
        """
        Format differences between schemas.

        Args:
            schema1_lines: Lines of the first schema
            schema2_lines: Lines of the second schema
            name1: Name/label of the first schema
            name2: Name/label of the second schema
            **kwargs: Additional formatting parameters

        Returns:
            Formatted string with differences
        """
        pass


class UnifiedDiffFormatter(BaseSchemaFormatter):
    """
    Unified diff style formatter (like diff -u).

    Shows differences in compact format with context lines.
    """

    def format(
        self,
        schema1_lines: List[str],
        schema2_lines: List[str],
        name1: str,
        name2: str,
        context_lines: int = 3,
        **kwargs: int,
    ) -> str:
        """
        Format in unified diff style.

        Args:
            schema1_lines: Lines of the first schema
            schema2_lines: Lines of the second schema
            name1: Name of the first schema
            name2: Name of the second schema
            context_lines: Number of context lines

        Returns:
            Formatted unified diff
        """
        diff_lines = list(
            unified_diff(
                schema1_lines,
                schema2_lines,
                fromfile=name1,
                tofile=name2,
                lineterm="",
                n=context_lines,
            )
        )

        if not diff_lines:
            return "Schemas are identical"

        # Apply colors and character-level highlighting
        result = []
        i = 0
        while i < len(diff_lines):
            line = diff_lines[i]

            if line.startswith("---") or line.startswith("+++"):
                result.append(self.format_line_with_color(line, "cyan", bold=True))
            elif line.startswith("@@"):
                result.append(self.format_line_with_color(line, "cyan"))
            elif (
                line.startswith("-")
                and i + 1 < len(diff_lines)
                and diff_lines[i + 1].startswith("+")
            ):
                # Pair of changed lines - highlight differences at character level
                line1 = line[1:]  # Remove '-' prefix
                line2 = diff_lines[i + 1][1:]  # Remove '+' prefix

                highlighted1, highlighted2, markers = self.highlight_line_differences(
                    line1, line2
                )

                result.append("-" + highlighted1)
                result.append("+" + highlighted2)
                # Show markers only in non-colored mode
                if not self.colored_output and markers:
                    result.append(" " + markers)

                i += 2  # Skip next line as we've already processed it
                continue
            elif line.startswith("-"):
                result.append(self.format_line_with_color(line, "red"))
            elif line.startswith("+"):
                result.append(self.format_line_with_color(line, "green"))
            else:
                result.append(line)

            i += 1

        return "\n".join(result)


class SideBySideDiffFormatter(BaseSchemaFormatter):
    """
    Side-by-side formatter (like diff -y).

    Shows schemas side by side for easy comparison.
    """

    def _visible_length(self, text: str) -> int:
        """
        Calculate visible length of string, excluding ANSI escape sequences.

        Args:
            text: String that may contain ANSI escape sequences

        Returns:
            Visible length of the string
        """
        ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
        return len(ansi_escape.sub("", text))

    def _pad_to_width(self, text: str, width: int) -> str:
        """
        Pad string to specified width accounting for ANSI escape sequences.

        Args:
            text: String to pad
            width: Target width

        Returns:
            Padded string
        """
        visible_len = self._visible_length(text)
        if visible_len >= width:
            return text
        padding = " " * (width - visible_len)
        return text + padding

    def format(
        self,
        schema1_lines: List[str],
        schema2_lines: List[str],
        name1: str,
        name2: str,
        width: int = DEFAULT_SIDE_BY_SIDE_WIDTH,
        **kwargs: int,
    ) -> str:
        """
        Format in side-by-side style.

        Args:
            schema1_lines: Lines of the first schema
            schema2_lines: Lines of the second schema
            name1: Name of the first schema
            name2: Name of the second schema
            width: Total output width

        Returns:
            Formatted side-by-side diff
        """
        col_width = (width - 3) // 2  # 3 characters for separator " | "

        result = []

        # Header
        header1 = name1[:col_width].ljust(col_width)
        header2 = name2[:col_width].ljust(col_width)
        if self.colored_output:
            header1 = colored(header1, "cyan", attrs=["bold"])
            header2 = colored(header2, "cyan", attrs=["bold"])
        result.append(f"{header1} | {header2}")
        result.append("-" * col_width + " | " + "-" * col_width)

        # Compare line by line
        max_lines = max(len(schema1_lines), len(schema2_lines))

        for i in range(max_lines):
            line1 = schema1_lines[i] if i < len(schema1_lines) else ""
            line2 = schema2_lines[i] if i < len(schema2_lines) else ""

            # Apply character-level highlighting if lines differ
            if line1 != line2 and line1 and line2:
                highlighted1, highlighted2, markers = self.highlight_line_differences(
                    line1, line2
                )
                # Truncate to column width based on visible length
                if self.colored_output:
                    if self._visible_length(highlighted1) > col_width:
                        highlighted1 = line1[:col_width]
                        highlighted1, _, _ = self.highlight_line_differences(
                            highlighted1,
                            line2[:col_width] if len(line2) > col_width else line2,
                        )
                    if self._visible_length(highlighted2) > col_width:
                        highlighted2 = line2[:col_width]
                        _, highlighted2, _ = self.highlight_line_differences(
                            line1[:col_width] if len(line1) > col_width else line1,
                            highlighted2,
                        )
                    line1_display = self._pad_to_width(highlighted1, col_width)
                    line2_display = self._pad_to_width(highlighted2, col_width)
                else:
                    # In non-colored mode show markers under differences
                    line1_display = highlighted1[:col_width].ljust(col_width)
                    line2_display = highlighted2[:col_width].ljust(col_width)

                result.append(f"{line1_display} | {line2_display}")

                # Add marker line in non-colored mode
                if not self.colored_output and markers:
                    markers_truncated = markers[:col_width]
                    markers_display = markers_truncated.ljust(col_width)
                    result.append(f"{markers_display} | {markers_display}")
            else:
                # Truncate or pad lines
                line1_truncated = line1[:col_width]
                line2_truncated = line2[:col_width]

                line1_display = line1_truncated.ljust(col_width)
                line2_display = line2_truncated.ljust(col_width)

                # Apply colors if lines differ (but without character highlighting)
                if self.colored_output and line1 != line2:
                    if line1 and not line2:
                        line1_display = colored(line1_display, "red")
                    elif line2 and not line1:
                        line2_display = colored(line2_display, "green")
                    elif line1 != line2:
                        line1_display = colored(line1_display, "red")
                        line2_display = colored(line2_display, "green")

                result.append(f"{line1_display} | {line2_display}")

        return "\n".join(result)


class NormalDiffFormatter(BaseSchemaFormatter):
    """
    Normal diff style formatter.

    Shows differences in simple format with < and > prefixes.
    """

    def format(
        self,
        schema1_lines: List[str],
        schema2_lines: List[str],
        name1: str,
        name2: str,
        **kwargs: int,
    ) -> str:
        """
        Format in normal diff style.

        Args:
            schema1_lines: Lines of the first schema
            schema2_lines: Lines of the second schema
            name1: Name of the first schema (not used in this formatter)
            name2: Name of the second schema (not used in this formatter)

        Returns:
            Formatted normal diff
        """
        if schema1_lines == schema2_lines:
            return "Schemas are identical"

        result = []

        # Simple line-by-line comparison
        max_lines = max(len(schema1_lines), len(schema2_lines))

        for i in range(max_lines):
            line1 = schema1_lines[i] if i < len(schema1_lines) else None
            line2 = schema2_lines[i] if i < len(schema2_lines) else None

            if line1 != line2:
                if line1 is not None and line2 is not None:
                    # Both lines exist - show character-level differences
                    highlighted1, highlighted2, markers = (
                        self.highlight_line_differences(line1, line2)
                    )

                    prefix1 = f"< {highlighted1}"
                    prefix2 = f"> {highlighted2}"

                    if self.colored_output:
                        # Already colored via highlight_line_differences
                        result.append(prefix1)
                        result.append(prefix2)
                    else:
                        # In non-colored mode show markers
                        result.append(prefix1)
                        result.append(prefix2)
                        if markers:
                            result.append(
                                "  " + markers
                            )  # 2 spaces to align with "< " and "> "

                    result.append("---")
                else:
                    # Only one line exists
                    if line1 is not None:
                        prefix = f"< {line1}"
                        if self.colored_output:
                            prefix = colored(prefix, "red")
                        result.append(prefix)

                    if line2 is not None:
                        prefix = f"> {line2}"
                        if self.colored_output:
                            prefix = colored(prefix, "green")
                        result.append(prefix)

                    if line1 is not None or line2 is not None:
                        result.append("---")

        return "\n".join(result) if result else "Schemas are identical"

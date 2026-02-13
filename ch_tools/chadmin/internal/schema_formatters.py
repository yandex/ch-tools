"""
Форматеры для вывода различий между схемами таблиц ClickHouse.

Предоставляет базовый класс и конкретные реализации форматеров
для отображения различий в разных стилях (unified, side-by-side, normal).
"""

import re
from abc import ABC, abstractmethod
from difflib import SequenceMatcher, unified_diff
from typing import List, Tuple

from termcolor import colored

# Константы
DEFAULT_SIDE_BY_SIDE_WIDTH = 160


class BaseSchemaFormatter(ABC):
    """
    Базовый класс для форматеров схем.

    Предоставляет общую функциональность для всех форматеров,
    включая подсветку различий на уровне символов и применение цветов.
    """

    def __init__(self, colored_output: bool = True):
        """
        Args:
            colored_output: Использовать ли цветной вывод
        """
        self.colored_output = colored_output

    def format_line_with_color(self, line: str, color: str, bold: bool = False) -> str:
        """
        Применить цвет к строке.

        Args:
            line: Строка для форматирования
            color: Цвет (red, green, cyan и т.д.)
            bold: Использовать ли жирный шрифт

        Returns:
            Отформатированная строка (с цветом если colored_output=True)
        """
        if not self.colored_output:
            return line

        attrs = ["bold"] if bold else []
        return colored(line, color, attrs=attrs)

    def highlight_line_differences(
        self, line1: str, line2: str
    ) -> Tuple[str, str, str]:
        """
        Подсветить различия между двумя строками на уровне символов.

        Args:
            line1: Первая строка
            line2: Вторая строка

        Returns:
            Кортеж (highlighted_line1, highlighted_line2, marker_line)
            где marker_line содержит символы "^" под различающимися символами
        """
        if line1 == line2:
            return line1, line2, ""

        matcher = SequenceMatcher(None, line1, line2)

        result1 = []
        result2 = []
        markers = []

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                # Одинаковые части - базовый цвет без жирного
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
                # Замененный текст - жирный на цветном фоне
                text1 = line1[i1:i2]
                text2 = line2[j1:j2]
                if self.colored_output:
                    result1.append(colored(text1, "red", attrs=["bold"]))
                    result2.append(colored(text2, "green", attrs=["bold"]))
                else:
                    result1.append(text1)
                    result2.append(text2)
                # Маркеры для более длинного сегмента
                max_len = max(i2 - i1, j2 - j1)
                markers.append("^" * max_len)
            elif tag == "delete":
                # Текст только в line1 - жирный красный
                text1 = line1[i1:i2]
                if self.colored_output:
                    result1.append(colored(text1, "red", attrs=["bold"]))
                else:
                    result1.append(text1)
                markers.append("^" * (i2 - i1))
            elif tag == "insert":
                # Текст только в line2 - жирный зеленый
                text2 = line2[j1:j2]
                if self.colored_output:
                    result2.append(colored(text2, "green", attrs=["bold"]))
                else:
                    result2.append(text2)
                # Для вставок нужно добавить отступ в result1 и маркеры
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
        Отформатировать различия между схемами.

        Args:
            schema1_lines: Строки первой схемы
            schema2_lines: Строки второй схемы
            name1: Имя/метка первой схемы
            name2: Имя/метка второй схемы
            **kwargs: Дополнительные параметры форматирования

        Returns:
            Отформатированная строка с различиями
        """
        pass


class UnifiedDiffFormatter(BaseSchemaFormatter):
    """
    Форматер в стиле unified diff (как diff -u).

    Показывает различия в компактном формате с контекстными строками.
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
        Отформатировать в стиле unified diff.

        Args:
            schema1_lines: Строки первой схемы
            schema2_lines: Строки второй схемы
            name1: Имя первой схемы
            name2: Имя второй схемы
            context_lines: Количество контекстных строк

        Returns:
            Отформатированный unified diff
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

        # Применить цвета и подсветку на уровне символов
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
                # Пара измененных строк - подсветить различия на уровне символов
                line1 = line[1:]  # Убрать префикс '-'
                line2 = diff_lines[i + 1][1:]  # Убрать префикс '+'

                highlighted1, highlighted2, markers = self.highlight_line_differences(
                    line1, line2
                )

                result.append("-" + highlighted1)
                result.append("+" + highlighted2)
                # Показать маркеры только в не-цветном режиме
                if not self.colored_output and markers:
                    result.append(" " + markers)

                i += 2  # Пропустить следующую строку, так как мы её обработали
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
    Форматер side-by-side (как diff -y).

    Показывает схемы рядом друг с другом для легкого сравнения.
    """

    def _visible_length(self, text: str) -> int:
        """
        Вычислить видимую длину строки, исключая ANSI escape-последовательности.

        Args:
            text: Строка, которая может содержать ANSI escape-последовательности

        Returns:
            Видимая длина строки
        """
        ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
        return len(ansi_escape.sub("", text))

    def _pad_to_width(self, text: str, width: int) -> str:
        """
        Дополнить строку до указанной ширины с учетом ANSI escape-последовательностей.

        Args:
            text: Строка для дополнения
            width: Целевая ширина

        Returns:
            Дополненная строка
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
        Отформатировать в стиле side-by-side.

        Args:
            schema1_lines: Строки первой схемы
            schema2_lines: Строки второй схемы
            name1: Имя первой схемы
            name2: Имя второй схемы
            width: Общая ширина вывода

        Returns:
            Отформатированный side-by-side diff
        """
        col_width = (width - 3) // 2  # 3 символа для разделителя " | "

        result = []

        # Заголовок
        header1 = name1[:col_width].ljust(col_width)
        header2 = name2[:col_width].ljust(col_width)
        if self.colored_output:
            header1 = colored(header1, "cyan", attrs=["bold"])
            header2 = colored(header2, "cyan", attrs=["bold"])
        result.append(f"{header1} | {header2}")
        result.append("-" * col_width + " | " + "-" * col_width)

        # Сравнить построчно
        max_lines = max(len(schema1_lines), len(schema2_lines))

        for i in range(max_lines):
            line1 = schema1_lines[i] if i < len(schema1_lines) else ""
            line2 = schema2_lines[i] if i < len(schema2_lines) else ""

            # Применить подсветку на уровне символов если строки различаются
            if line1 != line2 and line1 and line2:
                highlighted1, highlighted2, markers = self.highlight_line_differences(
                    line1, line2
                )
                # Обрезать до ширины колонки на основе видимой длины
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
                    # В не-цветном режиме показать маркеры под различиями
                    line1_display = highlighted1[:col_width].ljust(col_width)
                    line2_display = highlighted2[:col_width].ljust(col_width)

                result.append(f"{line1_display} | {line2_display}")

                # Добавить строку с маркерами в не-цветном режиме
                if not self.colored_output and markers:
                    markers_truncated = markers[:col_width]
                    markers_display = markers_truncated.ljust(col_width)
                    result.append(f"{markers_display} | {markers_display}")
            else:
                # Обрезать или дополнить строки
                line1_truncated = line1[:col_width]
                line2_truncated = line2[:col_width]

                line1_display = line1_truncated.ljust(col_width)
                line2_display = line2_truncated.ljust(col_width)

                # Применить цвета если строки различаются (но без подсветки символов)
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
    Форматер в стиле normal diff.

    Показывает различия в простом формате с префиксами < и >.
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
        Отформатировать в стиле normal diff.

        Args:
            schema1_lines: Строки первой схемы
            schema2_lines: Строки второй схемы
            name1: Имя первой схемы (не используется в этом форматере)
            name2: Имя второй схемы (не используется в этом форматере)

        Returns:
            Отформатированный normal diff
        """
        if schema1_lines == schema2_lines:
            return "Schemas are identical"

        result = []

        # Простое построчное сравнение
        max_lines = max(len(schema1_lines), len(schema2_lines))

        for i in range(max_lines):
            line1 = schema1_lines[i] if i < len(schema1_lines) else None
            line2 = schema2_lines[i] if i < len(schema2_lines) else None

            if line1 != line2:
                if line1 is not None and line2 is not None:
                    # Обе строки существуют - показать различия на уровне символов
                    highlighted1, highlighted2, markers = (
                        self.highlight_line_differences(line1, line2)
                    )

                    prefix1 = f"< {highlighted1}"
                    prefix2 = f"> {highlighted2}"

                    if self.colored_output:
                        # Уже окрашено через highlight_line_differences
                        result.append(prefix1)
                        result.append(prefix2)
                    else:
                        # В не-цветном режиме показать маркеры
                        result.append(prefix1)
                        result.append(prefix2)
                        if markers:
                            result.append(
                                "  " + markers
                            )  # 2 пробела для выравнивания с "< " и "> "

                    result.append("---")
                else:
                    # Существует только одна строка
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

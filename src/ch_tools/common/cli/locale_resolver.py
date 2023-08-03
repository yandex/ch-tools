import locale
import os
import subprocess
from typing import List, Tuple

__all__ = [
    "LocaleResolver",
]


class LocaleResolver:
    """
    Sets the locale for Click. Otherwise, it may fail with an error like

    ```
    RuntimeError: Click discovered that you exported a UTF-8 locale
    but the locale system could not pick up from it because it does not exist.
    The exported locale is 'en_US.UTF-8' but it is not supported.
    ```
    """

    @staticmethod
    def resolve():
        lang, _ = locale.getlocale()
        locales, has_c, has_en_us = LocaleResolver._get_utf8_locales()

        langs = map(lambda loc: str.lower(loc[0]), locales)
        if lang is None or lang.lower() not in langs:
            if has_c:
                lang = "C"
            elif has_en_us:
                lang = "en_US"
            else:
                raise RuntimeError(
                    f'Locale "{lang}" is not supported. '
                    'We tried to use "C" and "en_US" but they\'re absent on your machine.',
                )

        for locale_ in locales:
            if lang != locale_[0]:
                continue

            os.environ["LC_ALL"] = f"{lang}.{locale_[1]}"
            os.environ["LANG"] = f"{lang}.{locale_[1]}"

    @staticmethod
    def _get_utf8_locales() -> Tuple[List[Tuple[str, str]], bool, bool]:
        try:
            with subprocess.Popen(
                ["locale", "-a"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="ascii",
                errors="replace",
            ) as proc:
                stdout, _ = proc.communicate()
        except OSError:
            stdout = ""

        langs = []
        encodings = []

        has_c = False
        has_en_us = False

        for line in stdout.splitlines():
            locale_ = line.strip()
            if not locale_.lower().endswith(("utf-8", "utf8")):
                continue

            lang, encoding = locale_.split(".")

            langs.append(lang)
            encodings.append(encoding)

            has_c |= lang.lower() == "c"
            has_en_us |= lang.lower() == "en_us"

        res = list(zip(langs, encodings))

        return res, has_c, has_en_us

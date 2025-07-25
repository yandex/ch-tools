import gzip
import io
import json
import subprocess
import sys
from typing import Any, Dict, List, Optional

import yaml
from requests.exceptions import RequestException

from ch_tools.common import logging
from ch_tools.common.clickhouse.client import ClickhouseClient, OutputFormat

from .utils import delayed


class DiagnosticsData:
    def __init__(self, hostname: str, normalize_queries: bool) -> None:
        self.hostname = hostname
        self.normalize_queries = normalize_queries
        self._sections: List[Dict[str, Any]] = [{"section": None, "data": {}}]

    @delayed
    def add_string(self, name: str, value: str, section: Optional[str] = None) -> None:
        self._section(section)[name] = {
            "type": "string",
            "value": value,
        }

    @delayed
    def add_url(self, name: str, value: str, section: Optional[str] = None) -> None:
        self._section(section)[name] = {
            "type": "url",
            "value": value,
        }

    @delayed
    def add_xml_document(
        self, name: str, document: str, section: Optional[str] = None
    ) -> None:
        self._section(section)[name] = {
            "type": "xml",
            "value": document,
        }

    @delayed
    def add_query(
        self, name: str, query: str, result: str, section: Optional[str] = None
    ) -> None:
        self.add_query_sync(name, query, result, section)

    def add_query_sync(
        self, name: str, query: str, result: str, section: Optional[str] = None
    ) -> None:
        self._section(section)[name] = {
            "type": "query",
            "query": query,
            "result": result,
        }

    @delayed
    def add_command(
        self, name: str, command: str, result: str, section: Optional[str] = None
    ) -> None:
        self.add_command_sync(name, command, result, section)

    def add_command_sync(
        self, name: str, command: str, result: str, section: Optional[str] = None
    ) -> None:
        self._section(section)[name] = {
            "type": "command",
            "command": command,
            "result": result,
        }

    def dump(self, format_: str) -> None:
        if format_.startswith("json"):
            result = self._dump_json()
        elif format_.startswith("yaml"):
            result = self._dump_yaml()
        else:
            result = self._dump_wiki()

        if format_.endswith(".gz"):
            compressor = gzip.GzipFile(mode="wb", fileobj=sys.stdout.buffer)
            compressor.write(result.encode())
        else:
            logging.info(result)

    def _section(self, name: Optional[str] = None) -> Dict[str, Any]:
        if self._sections[-1]["section"] != name:
            self._sections.append({"section": name, "data": {}})

        return self._sections[-1]["data"]

    def _dump_json(self) -> str:
        """
        Dump diagnostic data in JSON format.
        """
        return json.dumps(self._sections, indent=2, ensure_ascii=False)

    def _dump_yaml(self) -> str:
        """
        Dump diagnostic data in YAML format.
        """
        return yaml.dump(self._sections, default_flow_style=False, allow_unicode=True)

    def _dump_wiki(self) -> str:
        """
        Dump diagnostic data in Yandex wiki format.
        """

        def _write_title(buffer_: io.StringIO, value: str) -> None:
            buffer_.write(f"===+ {value}\n")

        def _write_subtitle(buffer_: io.StringIO, value: str) -> None:
            buffer_.write(f"====+ {value}\n")

        def _write_string_item(
            buffer_: io.StringIO, name_: str, item_: Dict[str, str]
        ) -> None:
            value = item_["value"]
            if value != "":
                value = f"**{value}**"
            buffer_.write(f"{name_}: {value}\n")

        def _write_url_item(
            buffer_: io.StringIO, name_: str, item_: Dict[str, str]
        ) -> None:
            value = item_["value"]
            buffer_.write(f"**{name_}**\n{value}\n")

        def _write_xml_item(
            buffer_: io.StringIO,
            section_name_: Optional[str],
            name_: str,
            item_: Dict[str, str],
        ) -> None:
            if section_name_:
                buffer_.write(f"=====+ {name_}\n")
            else:
                _write_subtitle(buffer_, name_)

            _write_result(buffer_, item_["value"], format_="XML")

        def _write_query_item(
            buffer_: io.StringIO,
            section_name_: Optional[str],
            name_: str,
            item_: Dict[str, str],
        ) -> None:
            if section_name_:
                buffer_.write(f"=====+ {name_}\n")
            else:
                _write_subtitle(buffer_, name_)

            _write_query(buffer_, item_["query"])
            _write_result(buffer_, item_["result"])

        def _write_command_item(
            buffer_: io.StringIO,
            section_name_: Optional[str],
            name_: str,
            item_: Dict[str, str],
        ) -> None:
            if section_name_:
                buffer_.write(f"=====+ {name_}\n")
            else:
                _write_subtitle(buffer_, name_)

            _write_command(buffer_, item_["command"])
            _write_result(buffer_, item_["result"])

        def _write_unknown_item(
            buffer_: io.StringIO,
            section_name_: Optional[str],
            name_: str,
            item_: Dict[str, Any],
        ) -> None:
            if section_name_:
                buffer_.write(f"**{name_}**\n")
            else:
                _write_subtitle(buffer_, name_)

            json.dump(item_, buffer_, indent=2)

        def _write_query(buffer_: io.StringIO, query: str) -> None:
            buffer_.write("<{ query\n")
            buffer_.write("%%(SQL)\n")
            buffer_.write(query)
            buffer_.write("\n%%\n")
            buffer_.write("}>\n\n")

        def _write_command(buffer_: io.StringIO, command: str) -> None:
            buffer_.write("<{ command\n")
            buffer_.write("%%\n")
            buffer_.write(command)
            buffer_.write("\n%%\n")
            buffer_.write("}>\n\n")

        def _write_result(
            buffer_: io.StringIO, result: str, format_: Optional[str] = None
        ) -> None:
            if format_:
                buffer_.write(f"%%({format_})\n")
            else:
                buffer_.write("%%\n")
            buffer_.write(result)
            buffer_.write("\n%%\n")

        buffer = io.StringIO()

        _write_title(buffer, f"Diagnostics data for host {self.hostname}")
        for section in self._sections:
            section_name = section["section"]
            if section_name:
                _write_subtitle(buffer, section_name)

            for name, item in section["data"].items():
                if item["type"] == "string":
                    _write_string_item(buffer, name, item)
                elif item["type"] == "url":
                    _write_url_item(buffer, name, item)
                elif item["type"] == "query":
                    _write_query_item(buffer, section_name, name, item)
                elif item["type"] == "command":
                    _write_command_item(buffer, section_name, name, item)
                elif item["type"] == "xml":
                    _write_xml_item(buffer, section_name, name, item)
                else:
                    _write_unknown_item(buffer, section_name, name, item)

        return buffer.getvalue()


@delayed
def add_query(
    diagnostics: DiagnosticsData,
    name: str,
    client: ClickhouseClient,
    query: str,
    format_: OutputFormat,
    section: Optional[str] = None,
) -> None:
    query_args = {
        "normalize_queries": diagnostics.normalize_queries,
    }
    query = client.render_query(query, **query_args)
    diagnostics.add_query_sync(
        name=name,
        query=query,
        result=execute_query(client, query, render_query=False, format_=format_),
        section=section,
    )


def execute_query(
    client: ClickhouseClient,
    query: str,
    render_query: bool = True,
    format_: OutputFormat = OutputFormat.Default,
) -> Any:
    if render_query:
        query = client.render_query(query)

    try:
        return client.query(
            query,
            settings={
                "allow_introspection_functions": 1,
            },
            format_=format_,
        )
    except RequestException as e:
        return repr(e) if e.response is None else e.response.text


@delayed
def add_command(
    diagnostics: DiagnosticsData,
    name: str,
    command: str,
    section: Optional[str] = None,
) -> None:
    diagnostics.add_command_sync(
        name=name, command=command, result=_execute_command(command), section=section
    )


def _execute_command(command: str, input_: Optional[bytes] = None) -> str:
    # pylint: disable=consider-using-with

    proc = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if isinstance(input_, str):
        input_ = input_.encode()

    stdout, stderr = proc.communicate(input=input_)

    if proc.returncode:
        return f"failed with exit code {proc.returncode}\n{stderr.decode()}"

    return stdout.decode()

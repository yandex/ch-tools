"""
Module responsible for template rendering.
"""
import json
import os
from typing import Optional

from jinja2 import BaseLoader, Environment, FileSystemLoader, StrictUndefined

from . import docker
from .clickhouse import ClickhouseClient
from .typing import ContextT
from .utils import context_to_dict, env_stage

TEMP_FILE_EXT = "temp~"


@env_stage("create", fail=True)
def render_configs(context: ContextT) -> None:
    """
    Render each template in the subtree.
    Each template is rendered in-place. As the framework operates in
    staging dir, this is easily reset by `make clean`, or `rm -fr staging`.
    """
    staging_dir = context.conf["staging_dir"]
    for service in context.conf["services"].values():
        for instance_name in service["instances"]:
            context.instance_name = instance_name
            instance_dir = f"{staging_dir}/images/{instance_name}"
            for root, _, files in os.walk(instance_dir):
                for basename in files:
                    if not basename.endswith(TEMP_FILE_EXT):
                        _render_file(context, root, basename)
    context.instance_name = None


def render_template(context: ContextT, text: str) -> str:
    """
    Render template passed as a string.
    """
    template = _environment(context).from_string(text)
    return template.render(context_to_dict(context))


def _render_file(context: ContextT, directory: str, basename: str) -> None:
    path = os.path.join(directory, basename)
    temp_file_path = f"{path}.{TEMP_FILE_EXT}"
    loader = FileSystemLoader(directory)
    environment = _environment(context, loader)
    jinja_context = context_to_dict(context)
    try:
        with open(temp_file_path, "w", encoding="utf-8") as temp_file:
            template = environment.get_template(basename)
            temp_file.write(template.render(jinja_context))
    except Exception as e:
        raise RuntimeError(f"Failed to render {path}") from e
    os.rename(temp_file_path, path)


def _environment(context: ContextT, loader: Optional[BaseLoader] = None) -> Environment:
    """
    Create Environment object.
    """

    def _get_file_size(container_name, path):
        container = docker.get_container(context, container_name)
        return docker.get_file_size(container, path)

    def _clickhouse_version(container_name):
        return ClickhouseClient(context, container_name).get_version()

    environment = Environment(
        autoescape=False,
        trim_blocks=False,
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        loader=loader,
    )

    environment.filters["json"] = lambda x: json.dumps(x, indent=4)

    environment.globals["get_file_size"] = _get_file_size
    environment.globals["clickhouse_version"] = _clickhouse_version

    return environment

"""
Module responsible for template rendering.
"""

import json
import os
import shutil
from typing import Optional

from jinja2 import BaseLoader, Environment, FileSystemLoader, StrictUndefined

from . import docker
from .clickhouse import clickhouse_client
from .typing import ContextT
from .utils import context_to_dict, env_stage

IGNORED_EXT_LIST = ["gpg"]


def render_template(context: ContextT, text: str) -> str:
    """
    Render template passed as a string.
    """
    template = _environment(context).from_string(text)
    return template.render(context_to_dict(context))


@env_stage("create", fail=True)
def render_docker_configs(context: ContextT) -> None:
    """
    Render templated Docker configs.
    """
    images_dir = context.conf["images_dir"]
    staging_dir = context.conf["staging_dir"]
    for service_name, conf in context.conf["services"].items():
        service_dir = os.path.join(images_dir, service_name)
        for instance_name in conf["instances"]:
            instance_dir = os.path.join(staging_dir, "images", instance_name)
            os.makedirs(instance_dir, exist_ok=True)
            for dirpath, dirnames, filenames in os.walk(service_dir):
                target_dir = os.path.join(
                    instance_dir, os.path.relpath(dirpath, start=service_dir)
                )
                for dirname in dirnames:
                    os.makedirs(os.path.join(target_dir, dirname), exist_ok=True)

                for filename in filenames:
                    source_path = os.path.join(dirpath, filename)
                    target_path = os.path.join(target_dir, filename)
                    if _is_template(source_path):
                        _render_file(
                            context=context,
                            source_path=source_path,
                            target_path=target_path,
                            instance_name=instance_name,
                        )
                    else:
                        shutil.copy(source_path, target_path)


def _is_template(source_path):
    for ignored_ext in IGNORED_EXT_LIST:
        if source_path.endswith(ignored_ext):
            return False

    return True


def _render_file(
    context: ContextT,
    source_path: str,
    target_path: str,
    instance_name: str,
) -> None:
    environment = _environment(context, FileSystemLoader("."))

    jinja_context = context_to_dict(context)
    jinja_context["instance_name"] = instance_name

    try:
        with open(target_path, "w", encoding="utf-8") as file:
            template = environment.get_template(source_path)
            file.write(template.render(jinja_context))
    except Exception as e:
        raise RuntimeError(f"Failed to render {target_path}") from e


def _environment(context: ContextT, loader: Optional[BaseLoader] = None) -> Environment:
    """
    Create Environment object.
    """

    def _get_file_size(container_name, path):
        container = docker.get_container(context, container_name)
        return docker.get_file_size(container, path)

    def _clickhouse_version(container_name):
        return clickhouse_client(context, container_name).get_version()

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

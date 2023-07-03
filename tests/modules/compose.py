"""
Docker Compose interface.
"""

import os
import random
import shlex
import subprocess

import docker
import yaml

from . import utils
from .typing import ContextT

DOCKER_API = docker.from_env()


@utils.env_stage("create", fail=True)
def build_images(context: ContextT) -> None:
    """
    Build images from staging/ for testing
    """
    for service in context.conf["services"].values():
        for cmd in service.get("prebuild_cmd", []):
            try:
                proc = subprocess.run(
                    [cmd],
                    shell=True,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                if proc.stdout:
                    print(
                        "Command stdout: ",
                        str(proc.stdout, errors="replace", encoding="utf-8"),
                    )
                if proc.stderr:
                    print(
                        "Command stderr: ",
                        str(proc.stderr, errors="replace", encoding="utf-8"),
                    )
            except subprocess.CalledProcessError as err:
                raise RuntimeError(
                    f"prebuild command {cmd} failed.\n"
                    f"stdout: {err.stdout}\n"
                    f"stderr: {err.stderr}",
                ) from err

    _call_compose(context.conf, "build")


@utils.env_stage("start", fail=True)
def startup_containers(context: ContextT) -> None:
    """
    Start up docker containers.
    """
    _call_compose(context.conf, "up -d")


@utils.env_stage("stop", fail=False)
def shutdown_containers(context: ContextT) -> None:
    """
    Shutdown and remove docker containers.
    """
    _call_compose(context.conf, "down --volumes")


@utils.env_stage("create", fail=True)
def create_config(context: ContextT) -> None:
    """
    Generate config file and write it.
    """
    compose_conf_path = _get_config_path(context.conf)
    compose_conf = _generate_compose_config(context.conf)
    _write_config(compose_conf_path, compose_conf)


def _write_config(path: str, compose_conf: dict) -> None:
    """
    Dumps compose config into a file in Yaml format.
    """
    catalog_name = os.path.dirname(path)
    os.makedirs(catalog_name, exist_ok=True)
    temp_file_path = (
        f"{catalog_name}/.docker-compose-conftest-{random.randint(0, 100)}.yaml"
    )
    with open(temp_file_path, "w", encoding="utf-8") as conf_file:
        yaml.dump(compose_conf, stream=conf_file, default_flow_style=False, indent=4)
    try:
        _validate_config(temp_file_path)
        os.rename(temp_file_path, path)
    except subprocess.CalledProcessError as err:
        raise RuntimeError("Unable to write config: validation failed.") from err

    # Remove config only if validated ok.
    try:
        os.unlink(temp_file_path)
    except FileNotFoundError:
        pass


def _get_config_path(conf: dict) -> str:
    """
    Return file path to docker compose config file.
    """
    return os.path.join(conf["staging_dir"], "docker-compose.yml")


def _validate_config(config_path: str) -> None:
    """
    Perform config validation by calling `docker-compose config`
    """
    _call_compose_on_config(config_path, "__config_test", "config")


def _generate_compose_config(config: dict) -> dict:
    """
    Create docker compose config.
    """
    compose_config: dict = {
        "version": "2",
        "networks": {
            "test_net": {
                "external": {
                    "name": config["network_name"],
                },
            },
        },
        "services": {},
    }

    for service in config["services"].values():
        for instance_name in service["instances"]:
            service_config = _generate_service_config(config, instance_name, service)
            compose_config["services"][instance_name] = service_config

    return compose_config


def _generate_service_config(
    config: dict, instance_name: str, instance_config: dict
) -> dict:
    """
    Generates a single service config based on name and instance config.

    All paths are relative to the location of compose-config.yaml
    (which is ./staging/compose-config.yaml by default)
    """
    staging_dir = config["staging_dir"]
    network_name = config["network_name"]

    volumes = [f"./images/{instance_name}/config:/config:rw"]
    # Take care of port forwarding
    ports_list = []
    for port in instance_config.get("expose", {}).values():
        ports_list.append(port)

    dependency_list = []
    for dependency in instance_config.get("depends_on", {}):
        for instance in config["services"][dependency]["instances"]:
            dependency_list.append(instance)

    service = {
        "build": {
            "context": "..",
            "dockerfile": f"{staging_dir}/images/{instance_name}/Dockerfile",
            "args": instance_config.get("args", []),
        },
        "image": f"{instance_name}:{network_name}",
        "hostname": instance_name,
        "domainname": network_name,
        "depends_on": dependency_list,
        # Networks. We use external anyway.
        "networks": instance_config.get("networks", ["test_net"]),
        "environment": instance_config.get("environment", []),
        # Nice container name with domain name part.
        # This results, however, in a strange rdns name:
        # the domain part will end up there twice.
        # Does not affect A or AAAA, though.
        "container_name": f"{instance_name}.{network_name}",
        # Ports exposure
        "ports": ports_list,
        "volumes": volumes + instance_config.get("volumes", []),
        # https://github.com/moby/moby/issues/12080
        "tmpfs": "/var/run",
        "external_links": instance_config.get("external_links", []),
    }

    return service


def _prepare_volumes(volumes: dict, local_basedir: str) -> list:
    """
    Form a docker-compose volume list, and create endpoints.
    """
    volume_list = []
    for props in volumes.values():
        # "local" params are expected to be relative to docker-compose.yaml, so prepend its location.
        os.makedirs(f'{local_basedir}/{props["local"]}', exist_ok=True)
        volume_list.append(f'{props["local"]}:{props["remote"]}:{props["mode"]}')
    return volume_list


def _call_compose(conf: dict, action: str) -> None:
    conf_path = _get_config_path(conf)
    project_name = conf["network_name"]

    _call_compose_on_config(conf_path, project_name, action)


def _call_compose_on_config(conf_path: str, project_name: str, action: str) -> None:
    """
    Execute docker-compose action by invoking `docker-compose`.
    """
    compose_cmd = f"docker-compose --file {conf_path} -p {project_name} {action}"
    # Note: build paths are resolved relative to config file location.
    subprocess.check_call(shlex.split(compose_cmd))

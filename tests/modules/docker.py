"""
Docker interface.
"""

import io
import os
import tarfile
import tempfile
from typing import Sequence, Tuple
from urllib.parse import urlparse

import docker
from docker.models.containers import Container
from parallel_runtime import ENVIRONMENT_LABEL

from . import utils
from .typing import ContextT

DOCKER_API = docker.from_env()


def get_containers(context: ContextT) -> Sequence[Container]:
    """
    Get containers.
    """
    return DOCKER_API.containers.list(
        all=True,
        filters={"label": f"{ENVIRONMENT_LABEL}={context.conf['network_name']}"},
    )


def get_container(context: ContextT, instance_name: str) -> Container:
    """
    Get container object by prefix.
    """
    network_name = context.conf["network_name"]
    return DOCKER_API.containers.get(f"{instance_name}.{network_name}")


def get_exposed_port(container: Container, port: int) -> Tuple[str, int]:
    """
    Get pair of (host, port) for connection to exposed port.
    """
    host_url = os.getenv("DOCKER_HOST") or ""
    host = urlparse(host_url.strip()).hostname or "127.0.0.1"

    binding = container.attrs["NetworkSettings"]["Ports"].get(f"{port}/tcp")
    if not binding:
        raise RuntimeError(f"Container {container.name} has no binding for port {port}")

    return host, binding[0]["HostPort"]


def put_file(container: Container, data: bytes, path: str) -> None:
    """
    Put provided bytes data to given path
    """
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode="w") as tar_data:
        tarinfo = tarfile.TarInfo(name=path)
        tarinfo.size = len(data)
        tar_data.addfile(tarinfo, io.BytesIO(data))

    container.put_archive(path="/", data=tarstream.getvalue())


def copy_container_path(container: Container, path: str, local_dir: str) -> None:
    """Stream a file or directory archive to disk before extracting its contents."""
    os.makedirs(local_dir, exist_ok=True)
    archive, _ = container.get_archive(path)
    with tempfile.TemporaryFile(dir=local_dir) as buffer:
        for chunk in archive:
            buffer.write(chunk)
        buffer.seek(0)
        with tarfile.open(mode="r", fileobj=buffer) as tar:
            tar.extractall(
                path=local_dir,
                members=(entry for entry in tar if entry.isfile() or entry.isdir()),
                filter="data",
            )


def get_file_size(container: Container, path: str) -> int:
    """
    Return size of the specified file inside the container.
    """
    output = container.exec_run(f'stat --format "%s" "{path}"')
    return int(output.decode())


@utils.env_stage("create", fail=True)
def create_network(context: ContextT) -> None:
    """
    Create docker network specified in the config.
    """
    conf = context.conf
    net_name = conf["network_name"]
    try:
        network = DOCKER_API.networks.get(net_name)
    except docker.errors.NotFound:
        network = None
    if network is not None:
        if (network.attrs.get("Labels") or {}).get(ENVIRONMENT_LABEL) != net_name:
            raise RuntimeError(f"Network {net_name} belongs to another environment")
        return
    net_opts = {
        "com.docker.network.bridge.enable_ip_masquerade": "true",
        "com.docker.network.bridge.enable_icc": "true",
    }
    DOCKER_API.networks.create(
        net_name,
        options=net_opts,
        enable_ipv6=False,
        labels={ENVIRONMENT_LABEL: net_name},
    )


@utils.env_stage("stop", fail=True)
def shutdown_network(context: ContextT) -> None:
    """
    Stop docker network(s).
    """
    name = context.conf["network_name"]
    try:
        network = DOCKER_API.networks.get(name)
    except docker.errors.NotFound:
        return
    if (network.attrs.get("Labels") or {}).get(ENVIRONMENT_LABEL) != name:
        raise RuntimeError(f"Refusing to remove unowned network {name}")
    network.remove()

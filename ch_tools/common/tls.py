import socket
import ssl
import subprocess
from datetime import datetime
from functools import lru_cache
from typing import List, Optional, Tuple

from OpenSSL.crypto import FILETYPE_PEM, dump_certificate, load_certificate

from ch_tools.common.result import CRIT, OK, WARNING, Result


def check_cert_on_ports(
    ports: List[str],
    crit: int,
    warn: int,
    chain: bool,
    path: str,
) -> Result:
    # pylint: disable=too-many-return-statements
    file_chain = read_file_cert_chain(path)
    file_certificate, _ = read_cert_file(path)

    for port in ports:
        try:
            addr: Tuple[str, int] = (socket.getfqdn(), int(port))
            cert: str = ssl.get_server_certificate(addr)
            certificate, days_to_expire = load_certificate_info(str.encode(cert))
        except Exception as e:
            return Result(WARNING, f"Failed to get certificate: {repr(e)}")

        if certificate != file_certificate:
            return Result(
                CRIT,
                f"certificates on {port} and {path} are different",
            )
        if chain:
            try:
                socket_chain = get_client_cert_chain(addr)
                if len(socket_chain) != len(file_chain):
                    return Result(
                        CRIT,
                        f"certificates on {port} and {path} have different chain length",
                    )
                for file_cert, socket_cert in zip(file_chain, socket_chain):
                    if file_cert != socket_cert:
                        return Result(
                            CRIT,
                            f"certificates on {port} and {path} have different chains",
                        )
            except Exception as e:
                return Result(WARNING, f"Failed to get certificate chain: {repr(e)}")
        if days_to_expire < crit:
            return Result(CRIT, f"certificate {port} expires in {days_to_expire} days")
        if days_to_expire < warn:
            return Result(
                WARNING, f"certificate {port} expires in {days_to_expire} days"
            )

    return Result(OK)


@lru_cache(maxsize=None)
def read_cert_file_content(path: str) -> bytes:
    cmd = ["sudo", "/bin/cat", path]
    return subprocess.check_output(cmd, shell=False)


def read_cert_file(path: str) -> Tuple[str, int]:
    stdout = read_cert_file_content(path)
    return load_certificate_info(stdout)


def read_file_cert_chain(path: str) -> List[str]:
    stdout = read_cert_file_content(path)
    return read_all_certs(stdout)


def read_all_certs(blob: bytes) -> List[str]:
    start_line = b"-----BEGIN CERTIFICATE-----"
    result = []
    cert_slots = blob.split(start_line)
    for single_pem_cert in cert_slots[1:]:
        cert = load_certificate(FILETYPE_PEM, start_line + single_pem_cert)
        result.append(dump_certificate(FILETYPE_PEM, cert).decode())
    return result


def load_certificate_info(certificate: bytes) -> Tuple[str, int]:
    x509 = load_certificate(FILETYPE_PEM, certificate)
    x509_not_after: Optional[bytes] = x509.get_notAfter()
    assert x509_not_after is not None
    expire_date = datetime.strptime(x509_not_after.decode("ascii"), "%Y%m%d%H%M%SZ")
    return (
        dump_certificate(FILETYPE_PEM, x509).decode(),
        (expire_date - datetime.now()).days,
    )


def get_client_cert_chain(addr: Tuple[str, int]) -> List[str]:
    # pylint: disable=consider-using-with

    cmd = [
        "openssl",
        "s_client",
        "-showcerts",
        "-connect",
        f"{addr[0]}:{addr[1]}",
    ]
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE  # type: ignore[arg-type]
    )
    stdout, _ = proc.communicate(input="".encode())
    return read_all_certs(stdout)

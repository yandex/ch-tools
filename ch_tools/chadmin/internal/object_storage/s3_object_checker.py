"""Reusable S3 object probes for object-storage maintenance commands."""

from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from botocore.exceptions import (
    ClientError,
    CredentialRetrievalError,
    NoCredentialsError,
    PartialCredentialsError,
)

from ch_tools.chadmin.internal.object_storage.s3_object_metadata import (
    get_object_storage_key,
)

READ_CHUNK_SIZE = 8 * 1024 * 1024
MISSING_OBJECT_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}
INVALID_RANGE_ERROR_CODES = {"416", "InvalidRange", "RequestedRangeNotSatisfiable"}
FATAL_OBJECT_ERROR_CODES = {
    "AccessDenied",
    "AllAccessDisabled",
    "AuthorizationHeaderMalformed",
    "ExpiredToken",
    "InvalidAccessKeyId",
    "InvalidBucketName",
    "IllegalLocationConstraintException",
    "InvalidToken",
    "NoSuchBucket",
    "SignatureDoesNotMatch",
    "401",
    "403",
}
MAX_CACHED_OBJECT_PROBES = 10_000


@dataclass(frozen=True)
class ObjectProbe:
    exists: bool
    reported_size: Optional[int] = None
    read_size: Optional[int] = None


class ObjectCheckStatus(str, Enum):
    OK = "ok"
    MISSING = "missing"
    CORRUPTED = "corrupted"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ObjectCheckResult:
    key: str
    expected_size: int
    status: ObjectCheckStatus
    reason: str
    reported_size: Optional[int] = None
    read_size: Optional[int] = None
    error: str = ""


class S3ObjectChecker:
    """Probe S3 objects, caching only results when parts share a remote blob."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        prefix: str,
        method: str,
    ) -> None:
        if method not in {"head", "get", "range", "full"}:
            raise ValueError(f"Unsupported S3 check method: {method}")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.method = method
        self.cache: OrderedDict[str, ObjectProbe] = OrderedDict()

    def check(self, object_info: Any) -> Optional[str]:
        """Keep the checked-attach API and its strict HEAD/GET semantics."""
        key = get_object_storage_key(self.prefix, object_info)
        probe = self._cached_probe(key, object_info.size)

        if not probe.exists:
            return f"S3 object is missing: {key}"
        if probe.reported_size != object_info.size:
            return (
                f"S3 object size mismatch for {key}: expected {object_info.size}, "
                f"reported {probe.reported_size}"
            )
        if self.method in {"get", "full"} and probe.read_size != object_info.size:
            return (
                f"S3 object read size mismatch for {key}: expected "
                f"{object_info.size}, read {probe.read_size}"
            )
        return None

    def inspect(self, object_info: Any) -> ObjectCheckResult:
        """Return a non-destructive, machine-readable object check result."""
        key = get_object_storage_key(self.prefix, object_info)
        try:
            probe = self._cached_probe(key, object_info.size)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in FATAL_OBJECT_ERROR_CODES:
                raise
            if self.method == "range" and code in INVALID_RANGE_ERROR_CODES:
                return ObjectCheckResult(
                    key=key,
                    expected_size=object_info.size,
                    status=ObjectCheckStatus.CORRUPTED,
                    reason="range_not_satisfiable",
                    error=repr(error),
                )
            return ObjectCheckResult(
                key=key,
                expected_size=object_info.size,
                status=ObjectCheckStatus.UNKNOWN,
                reason="request_error",
                error=repr(error),
            )
        except (NoCredentialsError, PartialCredentialsError, CredentialRetrievalError):
            raise
        except Exception as error:  # Streaming errors vary between HTTP backends.
            return ObjectCheckResult(
                key=key,
                expected_size=object_info.size,
                status=ObjectCheckStatus.UNKNOWN,
                reason="read_error",
                error=repr(error),
            )

        if not probe.exists:
            return ObjectCheckResult(
                key=key,
                expected_size=object_info.size,
                status=ObjectCheckStatus.MISSING,
                reason="not_found",
            )

        if self.method != "head":
            expected_read_size = (
                min(object_info.size, 1) if self.method == "range" else object_info.size
            )
            if probe.read_size != expected_read_size:
                return ObjectCheckResult(
                    key=key,
                    expected_size=object_info.size,
                    status=ObjectCheckStatus.CORRUPTED,
                    reason="size_mismatch",
                    reported_size=probe.reported_size,
                    read_size=probe.read_size,
                )

        return ObjectCheckResult(
            key=key,
            expected_size=object_info.size,
            status=ObjectCheckStatus.OK,
            reason="ok",
            reported_size=probe.reported_size,
            read_size=probe.read_size,
        )

    def _cached_probe(self, key: str, expected_size: int) -> ObjectProbe:
        cache_key = key if self.method != "range" else f"{key}\0{expected_size}"
        try:
            probe = self.cache.pop(cache_key)
        except KeyError:
            probe = self._probe(key, expected_size)
            if len(self.cache) >= MAX_CACHED_OBJECT_PROBES:
                self.cache.popitem(last=False)
        self.cache[cache_key] = probe
        return probe

    def _probe(self, key: str, expected_size: int) -> ObjectProbe:
        try:
            if self.method == "head":
                response = self.client.head_object(Bucket=self.bucket, Key=key)
                return ObjectProbe(
                    exists=True,
                    reported_size=_content_length(response),
                )

            get_kwargs = {"Bucket": self.bucket, "Key": key}
            if self.method == "range" and expected_size > 0:
                get_kwargs["Range"] = "bytes=0-0"
            response = self.client.get_object(**get_kwargs)
            body = response["Body"]
            read_size = 0
            try:
                if self.method == "range":
                    read_size = len(body.read(1))
                else:
                    while True:
                        chunk = body.read(READ_CHUNK_SIZE)
                        if not chunk:
                            break
                        read_size += len(chunk)
            finally:
                body.close()
            return ObjectProbe(
                exists=True,
                reported_size=_content_length(response),
                read_size=read_size,
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in MISSING_OBJECT_ERROR_CODES:
                return ObjectProbe(exists=False)
            raise


def _content_length(response: Dict[str, Any]) -> Optional[int]:
    value = response.get("ContentLength")
    return int(value) if value is not None else None

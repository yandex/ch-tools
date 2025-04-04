import re
import uuid

UUID_TOKEN = "UUID"
ENGINE_TOKEN = "ENGINE"
UUID_PATTERN = re.compile(r"UUID\s+'([a-f0-9-]+)'", re.IGNORECASE)


def _is_valid_uuid(uuid_str: str) -> bool:
    try:
        val = uuid.UUID(uuid_str)
    except ValueError:
        return False
    return str(val) == uuid_str


def parse_uuid(line: str) -> str:
    match = UUID_PATTERN.search(line)

    if not match:
        raise RuntimeError("Failed parse UUID from metadata.")

    result = match.group(1)
    if not _is_valid_uuid(result):
        raise RuntimeError("Failed parse UUID from metadata.")
    return result

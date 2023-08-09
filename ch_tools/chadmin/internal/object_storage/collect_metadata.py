import logging
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable

from ch_tools.chadmin.internal.object_storage.s3_local_metadata import (
    S3ObjectLocalMetaData,
)

ObjectKeyToMetadata = Dict[str, Dict[Path, S3ObjectLocalMetaData]]


def collect_metadata(paths: Iterable[Path]) -> ObjectKeyToMetadata:
    """
    Return dictionary of parsed metadata from local disk with access by S3 object key.
    """
    logging.debug("Metadata collecting...")
    res: ObjectKeyToMetadata = defaultdict(dict)

    for path in paths:
        if not path.exists():
            logging.debug("Metadata path `%s` is not exist", path)

    for file in chain.from_iterable(path.rglob("*") for path in paths if path.exists()):
        if not file.is_file():
            continue

        try:
            metadata = S3ObjectLocalMetaData.from_file(file)
        except Exception:
            continue

        for obj in metadata.objects:
            res[obj.key][file] = metadata

    logging.debug("Metadata collected")
    return res

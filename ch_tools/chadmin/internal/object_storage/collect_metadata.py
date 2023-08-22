import logging
import os
from collections import defaultdict
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
    logging.debug("Metadata collecting for paths: %s", str(paths))
    res: ObjectKeyToMetadata = defaultdict(dict)

    for top_dir in paths:
        if not top_dir.exists():
            logging.debug("Metadata top_dir `%s` is not exist", top_dir)
            continue

        for dirpath, _dirnames, filenames in os.walk(str(top_dir)):
            for filename in filenames:
                file_path = Path(dirpath) / filename
                try:
                    metadata = S3ObjectLocalMetaData.from_file(file_path)
                except Exception as error:
                    logging.debug(
                        "Skip error occured while reading metadata from `%s`: %s",
                        file_path,
                        repr(error),
                    )
                    continue

                for obj in metadata.objects:
                    res[obj.key][file_path] = metadata

    logging.debug("Metadata collected")
    return res

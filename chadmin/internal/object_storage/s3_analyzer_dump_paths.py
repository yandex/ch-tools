from dataclasses import dataclass
from pathlib import Path

OBJECTS_WITH_METADATA_DUMP = 'objects_with_metadata.txt'
OBJECTS_WITHOUT_METADATA_DUMP = 'objects_without_metadata.txt'
OBJECTS_NOT_FOUND_DUMP = 'objects_not_found.txt'
UNPARSED_METADATA_DUMP = 'metadata_files_unparsed.txt'


@dataclass
class S3AnalyzerDumpPaths:
    objects_with_metadata: Path
    objects_without_metadata: Path
    objects_not_found: Path
    metadata_files_unparsed: Path

    @classmethod
    def create(cls, dump_dir: Path) -> 'S3AnalyzerDumpPaths':
        return cls(
            objects_with_metadata=dump_dir / OBJECTS_WITH_METADATA_DUMP,
            objects_without_metadata=dump_dir / OBJECTS_WITHOUT_METADATA_DUMP,
            objects_not_found=dump_dir / OBJECTS_NOT_FOUND_DUMP,
            metadata_files_unparsed=dump_dir / UNPARSED_METADATA_DUMP,
        )

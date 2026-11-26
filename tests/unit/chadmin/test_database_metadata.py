from pathlib import Path

import pytest

from ch_tools.chadmin.cli import database_metadata


def test_replicated_metadata_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "db.sql"
    path.write_text(
        "ATTACH DATABASE _ UUID '12345678-1234-1234-1234-123456789012'\nENGINE = Atomic\n"
    )
    monkeypatch.setattr(database_metadata, "db_metadata_path", lambda name: str(path))
    metadata = database_metadata.parse_database_metadata(r"team's\archive")
    metadata.set_replicated()
    restored = database_metadata.parse_database_metadata(metadata.database_name)
    assert restored == metadata
    assert (
        r"Replicated('/clickhouse/team\'s\\archive', '{shard}', '{replica}')"
        in path.read_text()
    )

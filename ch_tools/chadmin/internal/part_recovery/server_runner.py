"""
Extract data from a recovered Wide MergeTree part using the running ClickHouse
server instead of clickhouse-local.

Flow
----
1. Resolve ``(database, table)`` from the part path UUID via ``system.tables``.
2. Fetch the live schema from ``system.columns`` (fallback: ``columns.txt``).
3. Parse ``ORDER BY`` / ``PARTITION BY`` from ``system.tables.create_table_query``.
4. Find a local disk on the server (``system.disks WHERE type='local'``).
5. Create a scratch table ``_chadmin_recover._recover_<rand>`` with
   ``ENGINE=MergeTree`` on the local disk.
6. Move the assembled part into the scratch table's ``detached/`` directory.
7. ``ALTER TABLE _recover_<rand> ATTACH PART '<part_name>'``.
8. ``SELECT ... FORMAT TSV`` → output file (streamed via HTTP).
9. ``DROP TABLE _chadmin_recover._recover_<rand>`` in a ``finally`` block.
"""

import os
import re
import secrets
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ch_tools.chadmin.internal.part_recovery.exceptions import CriticalLossError
from ch_tools.common import logging
from ch_tools.common.clickhouse.client.clickhouse_client import ClickhouseClient

# Database used for all scratch tables
_RECOVER_DB = "_chadmin_recover"

# Regex to extract ORDER BY clause from CREATE TABLE DDL
_ORDER_BY_RE = re.compile(
    r"\bORDER\s+BY\s+(.+?)(?=\s*(?:PARTITION\s+BY|PRIMARY\s+KEY|SAMPLE\s+BY|TTL|SETTINGS|$))",
    re.IGNORECASE | re.DOTALL,
)

# Regex to extract PARTITION BY clause
_PARTITION_BY_RE = re.compile(
    r"\bPARTITION\s+BY\s+(.+?)(?=\s*(?:ORDER\s+BY|PRIMARY\s+KEY|SAMPLE\s+BY|TTL|SETTINGS|$))",
    re.IGNORECASE | re.DOTALL,
)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def parse_columns_txt(columns_txt: Path) -> List[Tuple[str, str]]:
    """
    Parse ``columns.txt`` and return a list of ``(column_name, type_string)`` pairs.

    The format written by ClickHouse is::

        columns format version: 1
        N columns:
        `col_name` TypeName
        ...
    """
    columns: List[Tuple[str, str]] = []
    text = columns_txt.read_text(encoding="utf-8")
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r"^`(.+?)`\s+(.+)$", line)
        if m:
            columns.append((m.group(1), m.group(2)))
    return columns


def get_live_schema(
    client: ClickhouseClient, database: str, table: str
) -> List[Tuple[str, str]]:
    """
    Return ``[(name, type), ...]`` from ``system.columns`` for *database*.*table*.

    Returns an empty list if the table does not exist.
    """
    db_esc = database.replace("'", "\\'")
    tbl_esc = table.replace("'", "\\'")
    rows = client.query_json_data(
        f"SELECT name, type FROM system.columns "
        f"WHERE database = '{db_esc}' AND table = '{tbl_esc}' "
        f"ORDER BY position",
    )
    return [(r[0], r[1]) for r in rows]


def merge_schemas(
    live: List[Tuple[str, str]],
    from_columns_txt: List[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    """
    Choose the schema used for the scratch recovery table.

    The detached part's physical layout corresponds to its own ``columns.txt``;
    therefore ``columns.txt`` is authoritative for recovery.  The live schema is
    used only as a consistency signal and to warn when the source table drifted
    after the part was created.
    """
    if not from_columns_txt:
        if not live:
            logging.warning(
                "Neither columns.txt nor system.columns contains schema information."
            )
            return []
        logging.warning(
            "columns.txt contains no columns — falling back to live schema from system.columns."
        )
        return live

    if not live:
        logging.warning(
            "Source table not found in system.columns — using columns.txt for schema."
        )
        return from_columns_txt

    if live != from_columns_txt:
        logging.warning(
            "Schema mismatch between system.columns {} and columns.txt {} — "
            "using part schema from columns.txt.",
            live,
            from_columns_txt,
        )

    return from_columns_txt


def format_columns_txt(columns: List[Tuple[str, str]]) -> str:
    """
    Format a list of ``(name, type)`` pairs into ``columns.txt`` content.

    The format matches what ClickHouse writes::

        columns format version: 1
        N columns:
        `col_name` TypeName
        ...

    Backticks in column names are escaped by doubling them.
    """
    lines = ["columns format version: 1", f"{len(columns)} columns:"]
    for name, typ in columns:
        # Escape backticks in column names by doubling them (ClickHouse convention)
        escaped_name = name.replace("`", "``")
        lines.append(f"`{escaped_name}` {typ}")
    return "\n".join(lines) + "\n"


def _try_read_columns_from_part(
    part_path: str,
    source_label: str,
) -> Optional[List[Tuple[str, str]]]:
    """
    Try to read ``columns.txt`` from a single part path.

    Returns parsed columns or ``None`` if reading fails.
    """
    columns_path = Path(part_path) / "columns.txt"
    if not columns_path.exists():
        return None
    try:
        columns = parse_columns_txt(columns_path)
        if columns:
            logging.info(
                "Found columns.txt from {} part: {}",
                source_label,
                columns_path,
            )
            return columns
    except (OSError, IOError, ValueError) as exc:
        logging.debug("Cannot read columns.txt from {}: {}", columns_path, exc)
    return None


def find_columns_txt_from_other_part(
    client: ClickhouseClient,
    database: str,
    table: str,
    exclude_part_name: str,
) -> Optional[List[Tuple[str, str]]]:
    """
    Try to read ``columns.txt`` from another active or detached part of the same table.

    Returns parsed columns as ``[(name, type), ...]`` or ``None`` if no other part
    with a readable ``columns.txt`` is found.
    """
    # Try active parts first (ORDER BY modification_time DESC for determinism)
    db_esc = database.replace("'", "\\'")
    tbl_esc = table.replace("'", "\\'")
    part_esc = exclude_part_name.replace("'", "\\'")
    rows = client.query_json_data(
        f"SELECT path FROM system.parts "
        f"WHERE database = '{db_esc}' AND table = '{tbl_esc}' "
        f"AND active = 1 AND name != '{part_esc}' "
        f"ORDER BY modification_time DESC "
        f"LIMIT 1",
    )
    if rows:
        # query_json_data returns list of lists (JSONCompact format)
        result = _try_read_columns_from_part(rows[0][0], "active")
        if result is not None:
            return result

    # Fallback to detached parts (ORDER BY name for determinism)
    rows = client.query_json_data(
        f"SELECT path FROM system.detached_parts "
        f"WHERE database = '{db_esc}' AND table = '{tbl_esc}' "
        f"AND name != '{part_esc}' "
        f"ORDER BY name "
        f"LIMIT 1",
    )
    if rows:
        result = _try_read_columns_from_part(rows[0][0], "detached")
        if result is not None:
            return result

    return None


def reconstruct_columns_txt(
    client: ClickhouseClient,
    part_path: Path,
    assembled_dir: Path,
) -> str:
    """
    Reconstruct missing ``columns.txt`` in *assembled_dir*.

    Strategy:
    1. Resolve the source table (database, table) from the part path UUID.
    2. Try to read ``columns.txt`` from another active/detached part of the same table.
    3. Fallback to live schema from ``system.columns``.
    4. If neither source is available, raise ``CriticalLossError``.

    Returns ``source_description`` indicating where the schema was taken from
    (e.g. "another active part", "system.columns").

    Raises
    ------
    CriticalLossError:
        If the source table cannot be resolved or no schema source is found.
    """

    part_name = part_path.name

    # Step 1: Resolve table
    try:
        database, table = resolve_table_from_path(client, part_path)
        logging.info("Resolved source table for columns.txt: {}.{}", database, table)
    except RuntimeError as exc:
        raise CriticalLossError(
            f"Cannot resolve source table for part {part_name}: {exc}. "
            "columns.txt is missing and no schema source is available."
        ) from exc

    # Step 2: Try columns.txt from another part
    columns = find_columns_txt_from_other_part(client, database, table, part_name)
    source = "another part"

    # Step 3: Fallback to system.columns
    if columns is None:
        logging.warning(
            "No columns.txt found from other parts of {}.{} — falling back to system.columns",
            database,
            table,
        )
        columns = get_live_schema(client, database, table)
        source = "system.columns"

    # Step 4: Fail if no schema
    if not columns:
        raise CriticalLossError(
            f"Cannot reconstruct columns.txt for part {part_name}: "
            f"table {database}.{table} has no schema in system.columns "
            "and no other part with columns.txt found."
        )

    # Write columns.txt
    columns_file = assembled_dir / "columns.txt"
    content = format_columns_txt(columns)
    columns_file.write_text(content, encoding="utf-8")
    logging.info(
        "Reconstructed columns.txt for part {} from {} ({} columns)",
        part_name,
        source,
        len(columns),
    )
    return source


# ---------------------------------------------------------------------------
# Table resolution
# ---------------------------------------------------------------------------


def resolve_table_from_path(
    client: ClickhouseClient, part_path: Path
) -> Tuple[str, str]:
    """
    Resolve ``(database, table)`` for the table that owns *part_path*.

    Strategy: extract the UUID directory component from the path
    (``store/<3-char-prefix>/<uuid>/``) and look it up in ``system.tables``.

    Falls back to parsing the path components if UUID lookup fails.

    Raises ``RuntimeError`` if the table cannot be resolved.
    """
    # Try to extract UUID from path like:
    #   /var/lib/clickhouse/disks/object_storage/store/abc/abcdef12-..../detached/broken_...
    #   /var/lib/clickhouse/store/abc/abcdef12-.../detached/broken_...
    uuid_match = re.search(
        r"/store/[0-9a-f]{3}/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/",
        str(part_path),
    )
    if uuid_match:
        uuid = uuid_match.group(1)
        uuid_esc = uuid.replace("'", "\\'")
        rows = client.query_json_data(
            f"SELECT database, name FROM system.tables WHERE uuid = '{uuid_esc}'",
        )
        if rows:
            return str(rows[0][0]), str(rows[0][1])
        logging.warning(
            "UUID {} not found in system.tables — table may have been dropped.", uuid
        )

    raise RuntimeError(
        f"Cannot resolve (database, table) from part path: {part_path}. "
        "The table may have been dropped. "
        "Ensure the source table exists or provide schema via columns.txt."
    )


def parse_order_by(create_table_query: str) -> str:
    """
    Extract the ORDER BY expression from a ``CREATE TABLE`` DDL string.

    Returns ``'tuple()'`` if not found.
    """
    m = _ORDER_BY_RE.search(create_table_query)
    if not m:
        return "tuple()"
    return m.group(1).strip()


def parse_partition_by(create_table_query: str) -> Optional[str]:
    """
    Extract the PARTITION BY expression from a ``CREATE TABLE`` DDL string.

    Returns ``None`` if not present.
    """
    m = _PARTITION_BY_RE.search(create_table_query)
    if not m:
        return None
    return m.group(1).strip()


# ---------------------------------------------------------------------------
# Disk selection
# ---------------------------------------------------------------------------


def find_local_disk(client: ClickhouseClient) -> str:
    """
    Return the name of the first local (non-object-storage) disk.

    ClickHouse reports local disks with type 'local' (older versions) or
    'LocalDisk' (newer versions, e.g. 26.x).  The built-in 'default' disk
    is always local and is used as a final fallback.

    Raises ``RuntimeError`` if no local disk is found.
    """
    rows = client.query_json_data(
        "SELECT name FROM system.disks "
        "WHERE type IN ('local', 'LocalDisk') "
        "ORDER BY name LIMIT 1"
    )
    if rows:
        return str(rows[0][0])

    # Fallback: check if 'default' disk exists (it is always local)
    default_rows = client.query_json_data(
        "SELECT name FROM system.disks WHERE name = 'default' LIMIT 1"
    )
    if default_rows:
        logging.warning(
            "No disk with type='local'/'LocalDisk' found; "
            "falling back to 'default' disk."
        )
        return "default"

    raise RuntimeError(
        "No local disk found on the ClickHouse server. "
        "The scratch recovery table requires a local disk. "
        "Ensure at least one disk of type='local' or 'LocalDisk' is configured."
    )


def get_disk_data_path(client: ClickhouseClient, disk_name: str) -> str:
    """Return the filesystem path of *disk_name* from ``system.disks``."""
    disk_esc = disk_name.replace("'", "\\'")
    rows = client.query_json_data(
        f"SELECT path FROM system.disks WHERE name = '{disk_esc}'",
    )
    if not rows:
        raise RuntimeError(f"Disk '{disk_name}' not found in system.disks.")
    return str(rows[0][0]).rstrip("/")


# ---------------------------------------------------------------------------
# Scratch table lifecycle
# ---------------------------------------------------------------------------


def _strip_broken_prefix(part_name: str) -> str:
    """Remove the ``broken_`` prefix from a detached part name."""
    prefix = "broken_"
    if part_name.startswith(prefix):
        return part_name[len(prefix) :]
    return part_name


def create_recover_db(client: ClickhouseClient) -> None:
    """Create the ``_chadmin_recover`` database if it does not exist."""
    client.query(f"CREATE DATABASE IF NOT EXISTS `{_RECOVER_DB}`")


def create_recover_table(
    client: ClickhouseClient,
    table_name: str,
    columns: List[Tuple[str, str]],
    order_by: str,
    partition_by: Optional[str],
    disk_name: str,
    broken_columns: Optional[Set[str]] = None,
) -> None:
    """
    Create the scratch MergeTree table ``_chadmin_recover.<table_name>``.

    Always uses plain ``MergeTree`` (never Replicated) on *disk_name*.

    Columns listed in *broken_columns* are excluded from the schema so that
    ``ATTACH PART`` does not try to read their (deleted) ``.bin`` / ``.mrk2``
    files.  Such columns will be substituted with ``NULL`` in the SELECT.
    """
    broken_columns = broken_columns or set()
    _validate_broken_columns_not_in_keys(order_by, partition_by, broken_columns)

    healthy_columns = [(n, t) for n, t in columns if n not in broken_columns]
    if not healthy_columns:
        raise RuntimeError(
            "Cannot create scratch recovery table: all columns are broken. "
            "There is no healthy data left to attach."
        )
    col_defs = ",\n    ".join(f"`{name}` {typ}" for name, typ in healthy_columns)
    partition_clause = f"\nPARTITION BY {partition_by}" if partition_by else ""
    sql = (
        f"CREATE TABLE `{_RECOVER_DB}`.`{table_name}` (\n"
        f"    {col_defs}\n"
        f")\n"
        f"ENGINE = MergeTree\n"
        f"ORDER BY {order_by}"
        f"{partition_clause}\n"
        f"SETTINGS disk = '{disk_name}'"
    )
    logging.debug("Creating scratch table:\n{}", sql)
    client.query(sql)


def _validate_broken_columns_not_in_keys(
    order_by: str,
    partition_by: Optional[str],
    broken_columns: Set[str],
) -> None:
    """Reject recovery plans where a broken column is used by table keys."""
    if not broken_columns:
        return

    expressions = [("ORDER BY", order_by)]
    if partition_by:
        expressions.append(("PARTITION BY", partition_by))

    offending: List[str] = []
    for clause_name, expression in expressions:
        for column in sorted(broken_columns):
            if _expression_references_column(expression, column):
                offending.append(f"{column} in {clause_name} {expression}")

    if offending:
        raise RuntimeError(
            "Cannot recover part because broken columns are used by table keys: "
            + "; ".join(offending)
        )


def _expression_references_column(expression: str, column: str) -> bool:
    """Return True if *expression* references *column* as an identifier."""
    backtick_pattern = rf"`{re.escape(column)}`"
    bare_pattern = rf"(?<![A-Za-z0-9_]){re.escape(column)}(?![A-Za-z0-9_])"
    return bool(
        re.search(backtick_pattern, expression) or re.search(bare_pattern, expression)
    )


def drop_recover_table(client: ClickhouseClient, table_name: str) -> None:
    """Drop the scratch table ``_chadmin_recover.<table_name>`` (best-effort)."""
    try:
        client.query(f"DROP TABLE IF EXISTS `{_RECOVER_DB}`.`{table_name}`")
        logging.info("Dropped scratch table {}.{}", _RECOVER_DB, table_name)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning(
            "Failed to drop scratch table {}.{}: {}", _RECOVER_DB, table_name, exc
        )


def place_part_in_detached(
    assembled_dir: Path,
    part_name: str,
    disk_data_path: str,
) -> Path:
    """
    Move the assembled part directory into the scratch table's ``detached/``
    directory on the local disk.

    *disk_data_path* is the table data path returned by
    ``get_table_data_path()`` (i.e. already includes the UUID store path).

    The placed files are chown'd to the owner of the parent ``detached/``
    directory so that the ClickHouse server (which runs as ``clickhouse``
    user) can read them after ``ATTACH PART``.

    Returns the destination path.
    """
    clean_name = _strip_broken_prefix(part_name)
    detached_dir = Path(disk_data_path) / "detached"
    detached_dir.mkdir(parents=True, exist_ok=True)
    dest = detached_dir / clean_name
    if dest.exists():
        shutil.rmtree(str(dest))
    shutil.copytree(str(assembled_dir), str(dest))

    # Lchown destination tree to match the ``detached/`` directory owner
    # (the ClickHouse server user).  Use os.lchown (not os.chown) so that
    # symlinks themselves are chown'd rather than their targets.
    # If we cannot chown (non-root or platform without chown), continue
    # silently — ClickHouse will fail later with a clear permission error.
    try:
        st = detached_dir.stat()
        target_uid, target_gid = st.st_uid, st.st_gid
        for root, dirs, files in os.walk(str(dest)):
            os.lchown(root, target_uid, target_gid)
            for d in dirs:
                os.lchown(os.path.join(root, d), target_uid, target_gid)
            for f in files:
                os.lchown(os.path.join(root, f), target_uid, target_gid)
    except (AttributeError, PermissionError, OSError) as exc:
        logging.warning(
            "Could not lchown placed part {} to ClickHouse user: {}",
            dest,
            exc,
        )

    logging.info("Placed part '{}' in detached: {}", clean_name, dest)
    return dest


def get_table_data_path(client: ClickhouseClient, table_name: str) -> str:
    """
    Return the first data path of ``_chadmin_recover.<table_name>``
    from ``system.tables``.
    """
    db_esc = _RECOVER_DB.replace("'", "\\'")
    tbl_esc = table_name.replace("'", "\\'")
    rows = client.query_json_data(
        f"SELECT data_paths[1] FROM system.tables "
        f"WHERE database = '{db_esc}' AND name = '{tbl_esc}'",
    )
    if not rows:
        raise RuntimeError(
            f"Cannot find data path for {_RECOVER_DB}.{table_name} in system.tables."
        )
    return str(rows[0][0]).rstrip("/")


# Regex for valid MergeTree part names: <partition>_<min>_<max>_<level>[_<mutation>]
# Only alphanumeric characters and underscores are allowed.
_PART_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def attach_part(client: ClickhouseClient, table_name: str, part_name: str) -> None:
    """Run ``ALTER TABLE _recover_<rand> ATTACH PART '<part_name>'``.

    *part_name* is validated against a strict regex before being embedded in
    the SQL string to prevent SQL injection via crafted part directory names.
    """
    clean_name = _strip_broken_prefix(part_name)
    if not _PART_NAME_RE.match(clean_name):
        raise ValueError(
            f"Part name '{clean_name}' contains unexpected characters. "
            "Expected only alphanumeric characters and underscores."
        )
    # Single-quote the part name; the regex above guarantees no embedded quotes.
    sql = f"ALTER TABLE `{_RECOVER_DB}`.`{table_name}` ATTACH PART '{clean_name}'"
    logging.info("Attaching part '{}' to scratch table …", clean_name)
    client.query(sql)


# ---------------------------------------------------------------------------
# SELECT → TSV
# ---------------------------------------------------------------------------


def build_select_sql(
    columns: List[Tuple[str, str]],
    broken_columns: Set[str],
    table_name: str,
) -> str:
    """
    Build a SELECT statement that substitutes NULL for broken columns.

    Broken columns are cast to ``Nullable(<original_type>)`` so that the TSV
    output contains ``\\N`` for those fields.
    """
    select_parts: List[str] = []
    for name, typ in columns:
        if name in broken_columns:
            inner_type = typ
            m = re.match(r"^Nullable\((.+)\)$", typ)
            if m:
                inner_type = m.group(1)
            select_parts.append(f"CAST(NULL AS Nullable({inner_type})) AS `{name}`")
        else:
            select_parts.append(f"`{name}`")

    select_clause = ",\n    ".join(select_parts)
    return (
        f"SELECT\n"
        f"    {select_clause}\n"
        f"FROM `{_RECOVER_DB}`.`{table_name}`\n"
        f"FORMAT TSV"
    )


def select_to_tsv(
    client: ClickhouseClient,
    columns: List[Tuple[str, str]],
    broken_columns: Set[str],
    table_name: str,
    output_path: Path,
) -> int:
    """
    Execute SELECT and stream TSV output to *output_path*.

    Returns the approximate number of rows written (based on newline-delimited output).

    The count may undercount the final line if the file does not end with a newline
    and will include trailing empty lines.
    """
    sql = build_select_sql(columns, broken_columns, table_name)
    logging.debug("SELECT SQL:\n{}", sql)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use streaming HTTP response to avoid loading all data into memory
    response = client.query(sql, stream=True)

    # Approximate row count based on newline-delimited output; may undercount the final line
    # if the file does not end with a newline and will include trailing empty lines.
    approx_rows = 0
    with output_path.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                fh.write(chunk)
                approx_rows += chunk.count(b"\n")

    logging.info("Wrote {} rows to {}", approx_rows, output_path)
    return approx_rows


# ---------------------------------------------------------------------------
# Column labels for report
# ---------------------------------------------------------------------------


def build_tsv_column_labels(
    columns: List[Tuple[str, str]],
    broken_columns: Set[str],
) -> List[str]:
    """
    Return a list of column labels for the TSV header in the report.

    Broken columns are annotated with ``(NULL)``.
    """
    labels: List[str] = []
    for name, _ in columns:
        if name in broken_columns:
            labels.append(f"{name} (NULL)")
        else:
            labels.append(name)
    return labels


# ---------------------------------------------------------------------------
# High-level runner
# ---------------------------------------------------------------------------


def run_server_recovery(
    client: ClickhouseClient,
    part_path: Path,
    assembled_dir: Path,
    broken_columns: Set[str],
    output_path: Path,
    columns_txt: Path,
) -> Tuple[int, List[str]]:
    """
    Full server-side recovery flow.

    1. Resolve source table → live schema → merged schema.
    2. Create scratch table on local disk.
    3. Place assembled part in detached/.
    4. ATTACH PART.
    5. SELECT → TSV.
    6. DROP scratch table (always, in finally).

    Returns ``(rows_written, tsv_column_labels)``.
    """
    part_name = part_path.name

    # ── Step 1: Resolve table and schema ─────────────────────────────────────
    columns_from_txt = parse_columns_txt(columns_txt)

    database: Optional[str] = None
    table: Optional[str] = None
    live_schema: List[Tuple[str, str]] = []

    try:
        database, table = resolve_table_from_path(client, part_path)
        logging.info("Resolved source table: {}.{}", database, table)
        live_schema = get_live_schema(client, database, table)
    except RuntimeError as exc:
        logging.warning("Could not resolve source table: {}. Using columns.txt.", exc)

    columns = merge_schemas(live_schema, columns_from_txt)

    # ── Step 2: Get ORDER BY / PARTITION BY ───────────────────────────────────
    order_by = "tuple()"
    partition_by: Optional[str] = None

    if live_schema and database and table:
        db_esc = database.replace("'", "\\'")
        tbl_esc = table.replace("'", "\\'")
        rows = client.query_json_data(
            f"SELECT create_table_query FROM system.tables "
            f"WHERE database = '{db_esc}' AND name = '{tbl_esc}'",
        )
        if rows:
            ddl = str(rows[0][0])
            order_by = parse_order_by(ddl)
            partition_by = parse_partition_by(ddl)
            logging.info(
                "Using ORDER BY '{}', PARTITION BY '{}'", order_by, partition_by
            )

    # ── Step 3: Find local disk ───────────────────────────────────────────────
    disk_name = find_local_disk(client)
    logging.info("Using local disk '{}' for scratch table", disk_name)

    # ── Step 4: Create scratch table ──────────────────────────────────────────
    rand_suffix = secrets.token_hex(4)
    scratch_table = f"_recover_{rand_suffix}"

    create_recover_db(client)
    create_recover_table(
        client,
        scratch_table,
        columns,
        order_by,
        partition_by,
        disk_name,
        broken_columns=broken_columns,
    )
    logging.info("Created scratch table {}.{}", _RECOVER_DB, scratch_table)

    try:
        # ── Step 5: Place part in detached/ ───────────────────────────────────
        table_data_path = get_table_data_path(client, scratch_table)
        place_part_in_detached(assembled_dir, part_name, table_data_path)

        # ── Step 6: ATTACH PART ───────────────────────────────────────────────
        attach_part(client, scratch_table, part_name)

        # ── Step 7: SELECT → TSV ──────────────────────────────────────────────
        rows = select_to_tsv(
            client, columns, broken_columns, scratch_table, output_path
        )

        tsv_labels = build_tsv_column_labels(columns, broken_columns)
        return rows, tsv_labels

    finally:
        drop_recover_table(client, scratch_table)


# ---------------------------------------------------------------------------
# Disk-level path helpers (used by tests / integration)
# ---------------------------------------------------------------------------


def get_disk_names_by_type(client: ClickhouseClient) -> Dict[str, str]:
    """Return ``{disk_name: disk_type}`` for all disks."""
    rows = client.query_json_data("SELECT name, type FROM system.disks")
    return {str(r[0]): str(r[1]) for r in rows}

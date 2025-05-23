"""
Steps for interacting with ClickHouse DBMS.
"""

import os

from behave import when
from modules import s3
from modules.clickhouse import execute_query
from modules.steps import get_step_data


@when("we remove key from s3 for partitions database {database} on {node:w}")
def step_check_number_ro_replicas(context, database, node):
    data = get_step_data(context)
    keys_to_remove = []

    # Get the list of keys in s3 to broke specified partitions.
    for database, database_info in data.items():
        for table, table_info in database_info.items():
            for partition in table_info:
                get_parts_info_query = f"SELECT name, partition, path  FROM system.parts where database='{database}' and table='{table}' and partition='{partition}'"
                # Get local path on disk to the single data part.
                part_local_path = execute_query(
                    context, node, get_parts_info_query, format_="JSONCompact"
                )["data"][0][2]
                # For this part, get the single object key in s3.
                get_object_key_query = f"SELECT concat(path, local_path) AS full_path, remote_path from system.remote_data_paths  WHERE disk_name='object_storage'  and startsWith(full_path, '{os.path.join(part_local_path, 'columns.txt')}')"
                data_object_key = execute_query(
                    context, node, get_object_key_query, format_="JSONCompact"
                )["data"][0][1]
                keys_to_remove.append(data_object_key)

    s3_client = s3.S3Client(context)
    for key in keys_to_remove:
        s3_client.delete_data(key)

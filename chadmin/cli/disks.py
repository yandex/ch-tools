from click import group, option
import os
import re


@group('disks')
def disks_group():
    pass


@disks_group.command('check-s3-metadata')
@option('--path', 'path', default='/var/lib/clickhouse/disks/object_storage/store', help='Path to S3 metadata')
def check_s3_metadata_command(path):
    check_dir(path)


def check_dir(path):
    for (dirpath, _, filenames) in os.walk(path):
        for filename in filenames:
            if not check_file(f'{dirpath}/{filename}'):
                print(f'{dirpath}/{filename}')


def check_file(filename):
    file = open(filename, mode='r', encoding='latin-1')
    lines = file.readlines(1024)
    if len(lines) != 5:
        file.close()
        return False
    result = True
    if not re.match('[123]\n', lines[0]):  # version 1-3
        result = False
    elif not re.match('1\\s+\\d+\n', lines[1]):  # object count=1 & size
        result = False
    elif not re.match('\\d+\\s+\\S+\n', lines[2]):  # size & object name
        result = False
    elif not re.match('\\d+\n', lines[3]):  # refcount
        result = False
    elif not re.match('[01]\n?', lines[4]):  # is readonly
        result = False
    file.close()
    return result

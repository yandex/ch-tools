import pytest

from ch_tools.chadmin.internal.object_storage.obj_list_item import ObjListItem
from ch_tools.common.commands.clean_object_storage import _split_response_to_keys


def chunkstring(string, size):
    return (string[i : i + size] for i in range(0, len(string), size))


@pytest.mark.parametrize(
    "chunk_size",
    [
        8192,  # same as actual
        9,  # 1 line per chunk
        27,  # 3 lines
        14,  # split line in the middle
        8,  # full line except \n symbol
        7,
        1,
    ],
)
def test_split_response(chunk_size):
    response = b"data/a\t1\ndata/b\t2\ndata/c\t3\ndata/d\t4\ndata/e\t5\n"
    result_keys = [
        ObjListItem("data/a", 1),
        ObjListItem("data/b", 2),
        ObjListItem("data/c", 3),
        ObjListItem("data/d", 4),
        ObjListItem("data/e", 5),
    ]
    actual_keys = []
    last_line = ""

    for chunk in chunkstring(response, chunk_size):
        keys, last_line = _split_response_to_keys(chunk, last_line)
        actual_keys.extend(keys)

    assert result_keys == actual_keys

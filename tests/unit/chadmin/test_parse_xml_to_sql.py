from collections.abc import Callable
from pathlib import Path

import pytest

from ch_tools.chadmin.internal.dictionary_migration import (
    _generate_ddl_dictionaries_from_xml,
)


@pytest.fixture
def test_data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "test_dictionary.xml"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def create_xml_file(test_data_dir: Path) -> Callable[[str, str], str]:
    def _create(filename: str, content: str) -> str:
        filepath = test_data_dir / filename
        filepath.write_text(content)
        return str(filepath)

    return _create


@pytest.mark.parametrize(
    "xml_content,expected_queries",
    [
        pytest.param(
            """
<clickhouse>
  <dictionary>
    <name>test_dict1</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime><min>0</min><max>100</max></lifetime>
    <structure>
      <id><name>id</name></id>
      <attribute>
        <name>name</name>
        <type>String</type>
        <null_value></null_value>
      </attribute>
    </structure>
  </dictionary>
</clickhouse>
            """,
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict1
(
    id UInt64,
    name String DEFAULT ''
)
PRIMARY KEY id
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 100)
                """
            ],
            id="in-any-block",
        ),
        pytest.param(
            """
<clickhouse>
    <dictionary>
        <name>dictionary1</name>
        <source>
            <clickhouse>
                <host>name.db.yandex.net</host>
                <port>8123</port>
                <user>default</user>
                <password></password>
                <db>default</db>
                <table>test_dict</table>
            </clickhouse>
        </source>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>value</name>
                <type>UInt64</type>
                <null_value>0</null_value>
                <hierarchical>0</hierarchical>
                <injective>0</injective>
            </attribute>
        </structure>
        <layout>
            <flat/>
        </layout>
        <lifetime>50</lifetime>
    </dictionary>
</clickhouse>
            """,
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.dictionary1
(
    id UInt64,
    value UInt64 DEFAULT 0 HIERARCHICAL INJECTIVE
)
PRIMARY KEY id
SOURCE(clickhouse(HOST name.db.yandex.net PORT 8123 USER default PASSWORD '' DB default TABLE test_dict))
LAYOUT(FLAT())
LIFETIME(50)
                """
            ],
            id="large-config",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>test_dict1</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime><min>0</min><max>100</max></lifetime>
    <structure>
      <id><name>id</name></id>
      <attribute>
        <name>name</name>
        <type>String</type>
        <null_value></null_value>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>
            """,
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict1
(
    id UInt64,
    name String DEFAULT ''
)
PRIMARY KEY id
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 100)
                """
            ],
            id="empty-null-value",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>test_dict2</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime>100</lifetime>
    <structure>
      <id><name>id</name></id>
      <attribute>
        <name>name</name>
        <type>String</type>
        <null_value>Mama</null_value>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>""",
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict2
(
    id UInt64,
    name String DEFAULT 'Mama'
)
PRIMARY KEY id
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(FLAT())
LIFETIME(100)
           """
            ],
            id="explicit_null_value",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>test_dict3</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime>0</lifetime>
    <structure>
      <id><name>id</name></id>
      <attribute>
        <name>name</name>
        <type>String</type>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>
            """,
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict3
(
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(FLAT())
LIFETIME(0)
                """
            ],
            id="no_null_value_attribute",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>test_dict4</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime><min>0</min><max>100</max></lifetime>
    <structure>
      <id><name>id</name></id>
      <attribute>
        <name>name</name>
        <type>String</type>
        <null_value/>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>
            """,
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict4
(
    id UInt64,
    name String DEFAULT ''
)
PRIMARY KEY id
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 100)
                """
            ],
            id="self_closing_null_value",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>test_dict5</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><hashed/></layout>
    <lifetime><min>0</min><max>100</max></lifetime>
    <structure>
      <key>
        <attribute>
          <name>id1</name>
          <type>UInt64</type>
        </attribute>
        <attribute>
          <name>id2</name>
          <type>String</type>
        </attribute>
      </key>
      <attribute>
        <name>value</name>
        <type>Int32</type>
        <null_value></null_value>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>
            """,
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict5
(
    id1 UInt64,
    id2 String,
    value Int32 DEFAULT 0
)
PRIMARY KEY id1, id2
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 100)
                """
            ],
            id="composite-key",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>test_dict6</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tab</table>
      </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime><min>0</min><max>100</max></lifetime>
    <structure>
      <id><name>id</name></id>
      <attribute>
        <name>score</name>
        <type>Float64</type>
        <null_value></null_value>
      </attribute>
      <attribute>
        <name>date</name>
        <type>Date</type>
        <null_value/>
      </attribute>
      <attribute>
        <name>tags</name>
        <type>Array</type>
        <null_value></null_value>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>""",
            [
                """
CREATE DICTIONARY IF NOT EXISTS _dictionaries.test_dict6
(
    id UInt64,
    score Float64 DEFAULT 0,
    date Date DEFAULT 1970-01-01,
    tags Array DEFAULT []
)
PRIMARY KEY id
SOURCE(clickhouse(DB db TABLE tab))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 100)
                """
            ],
            id="multiple-types",
        ),
    ],
)
def test_parse_xml_to_sql(
    create_xml_file: Callable[[str, str], str],
    xml_content: str,
    expected_queries: list[str],
) -> None:
    filepath = create_xml_file("test_dict.xml", xml_content)
    results = [
        query
        for dict_name, query in _generate_ddl_dictionaries_from_xml(
            filepath, target_database="_dictionaries"
        )
    ]

    assert len(results) == len(expected_queries)

    for actual, expected in zip(results, expected_queries):
        assert normalize_sql(actual) == normalize_sql(expected)


def normalize_sql(sql: str) -> str:
    lines = [line.strip() for line in sql.strip().split("\n")]
    return "\n".join(line for line in lines if line)


@pytest.mark.parametrize(
    "xml_content,expected_count",
    [
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>dict1</name>
    <source>
        <clickhouse>
            <db>db</db>
            <table>t</table>
        </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime>0</lifetime>
    <structure><id><name>id</name></id></structure>
  </dictionary>
  <dictionary>
    <name>dict2</name>
    <source>
        <clickhouse>
            <db>db</db>
            <table>t</table>
        </clickhouse>
    </source>
    <layout><flat/></layout>
    <lifetime>0</lifetime>
    <structure><id><name>id</name></id></structure>
  </dictionary>
</dictionaries>
            """,
            2,
            id="multiple-dictionaries",
        ),
    ],
)
def test_parse_with_multiple_dictionaries(
    create_xml_file: Callable[[str, str], str], xml_content: str, expected_count: int
) -> None:
    filepath = create_xml_file("test_multiple.xml", xml_content)
    results = _generate_ddl_dictionaries_from_xml(
        filepath, target_database="_dictionaries"
    )
    assert len(results) == expected_count


@pytest.mark.parametrize(
    "xml_content",
    [
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tbl</table>
      </clickhouse>
    </source>
    <structure>
      <attribute>
        <name>value</name>
        <type>String</type>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>
""",
            id="missing-name",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>dict_without_source</name>
  </dictionary>
</dictionaries>
""",
            id="missing-source-or-structure",
        ),
        pytest.param(
            """
<dictionaries>
  <dictionary>
    <name>dict_with_id_and_key</name>
    <source>
      <clickhouse>
        <db>db</db>
        <table>tbl</table>
      </clickhouse>
    </source>
    <structure>
      <id>
        <name>id</name>
        <type>UInt64</type>
      </id>
      <key>
        <attribute>
          <name>k</name>
          <type>UInt64</type>
        </attribute>
      </key>
      <attribute>
        <name>value</name>
        <type>String</type>
      </attribute>
    </structure>
  </dictionary>
</dictionaries>
""",
            id="id-and-key-present",
        ),
    ],
)
def test_parse_xml_with_invalid_config(
    xml_content: str,
    create_xml_file: Callable[[str, str], str],
) -> None:
    filepath = create_xml_file("invalid_dictionary.xml", xml_content)

    with pytest.raises(RuntimeError):
        _generate_ddl_dictionaries_from_xml(filepath, target_database="_dictionaries")

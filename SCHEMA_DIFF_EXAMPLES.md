# Schema Diff Examples

Примеры использования команды `chadmin table schema-diff` с новыми опциями `--ignore-engine` и `--ignore-uuid`.

## Базовое использование

Сравнение двух таблиц в ClickHouse:
```bash
chadmin table schema-diff db1.table1 db2.table2
```

Сравнение таблицы с файлом:
```bash
chadmin table schema-diff db1.table1 /tmp/table_schema.sql
```

Сравнение двух файлов:
```bash
chadmin table schema-diff /tmp/schema1.sql /tmp/schema2.sql
```

Сравнение таблицы с ZooKeeper:
```bash
chadmin table schema-diff db1.table1 zk:/clickhouse/tables/shard1/table1
```

## Игнорирование UUID

Опция `--ignore-uuid` позволяет игнорировать различия в UUID таблиц при сравнении схем.

### Пример

Две таблицы с разными UUID, но одинаковой структурой:

**schema1.sql:**
```sql
CREATE TABLE test UUID '12345678-1234-1234-1234-123456789012' (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id
```

**schema2.sql:**
```sql
CREATE TABLE test UUID 'abcdefab-abcd-abcd-abcd-abcdefabcdef' (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id
```

Без опции `--ignore-uuid`:
```bash
chadmin table schema-diff schema1.sql schema2.sql
```
Результат покажет различия в UUID.

С опцией `--ignore-uuid`:
```bash
chadmin table schema-diff schema1.sql schema2.sql --ignore-uuid
```
Результат: `Schemas are identical` - UUID игнорируются.

## Игнорирование движка таблиц

Опция `--ignore-engine` позволяет игнорировать различия в движках таблиц при сравнении схем.

### Пример

Две таблицы с разными движками, но одинаковой структурой:

**schema1.sql:**
```sql
CREATE TABLE test (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id
```

**schema2.sql:**
```sql
CREATE TABLE test (
    id UInt64,
    name String
) ENGINE = ReplacingMergeTree()
ORDER BY id
```

Без опции `--ignore-engine`:
```bash
chadmin table schema-diff schema1.sql schema2.sql
```
Результат покажет различия в ENGINE.

С опцией `--ignore-engine`:
```bash
chadmin table schema-diff schema1.sql schema2.sql --ignore-engine
```
Результат: `Schemas are identical` - движки игнорируются.

## Комбинированное использование

Можно использовать обе опции одновременно для игнорирования и UUID, и движка:

```bash
chadmin table schema-diff db1.table1 db2.table2 --ignore-uuid --ignore-engine
```

### Пример

**schema1.sql:**
```sql
CREATE TABLE test UUID '12345678-1234-1234-1234-123456789012' (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id
```

**schema2.sql:**
```sql
CREATE TABLE test UUID 'abcdefab-abcd-abcd-abcd-abcdefabcdef' (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}')
ORDER BY id
```

С обеими опциями:
```bash
chadmin table schema-diff schema1.sql schema2.sql --ignore-uuid --ignore-engine
```
Результат: `Schemas are identical` - игнорируются и UUID, и движок (включая параметры ReplicatedMergeTree).

## Другие полезные опции

### Формат вывода

Side-by-side формат:
```bash
chadmin table schema-diff db1.table1 db2.table2 --format side-by-side
```

Normal формат:
```bash
chadmin table schema-diff db1.table1 db2.table2 --format normal
```

### Отключение цветного вывода

```bash
chadmin table schema-diff db1.table1 db2.table2 --no-color
```

### Количество строк контекста

```bash
chadmin table schema-diff db1.table1 db2.table2 --context-lines 5
```

### Отключение нормализации

```bash
chadmin table schema-diff db1.table1 db2.table2 --no-normalize
```

## Практические сценарии использования

### 1. Проверка миграции таблиц между кластерами

При миграции таблиц между кластерами UUID и параметры ReplicatedMergeTree могут отличаться, но структура должна быть идентичной:

```bash
chadmin table schema-diff \
  cluster1_db.users \
  cluster2_db.users \
  --ignore-uuid \
  --ignore-engine
```

### 2. Сравнение локальной и реплицированной версий таблицы

```bash
chadmin table schema-diff \
  local_db.table \
  replicated_db.table \
  --ignore-engine
```

### 3. Проверка соответствия схемы в файле и в базе

```bash
chadmin table schema-diff \
  production_db.users \
  /path/to/schema/users.sql \
  --ignore-uuid
```

## Заметки

- Опция `--normalize` (включена по умолчанию) автоматически удаляет параметры ReplicatedMergeTree при сравнении
- Опция `--ignore-engine` работает с любыми движками, включая параметризованные (например, `ReplacingMergeTree(version)`)
- Опция `--ignore-uuid` заменяет все UUID на placeholder `<ignored>` перед сравнением
- Обе опции можно комбинировать с любыми форматами вывода (`unified`, `side-by-side`, `normal`)
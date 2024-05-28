ATTACH TABLE _ UUID '72b4c520-9cc2-4549-ba6c-bd952bb049d8'
(
    `D` Date,
    `ID` Int64,
    `Ver` UInt64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/tableName/{shard}/1', '{replica}', Ver)
PARTITION BY toYYYYMM(D)
ORDER BY ID
SETTINGS index_granularity = 8192
ATTACH TABLE _ UUID '4ce817e2-8043-4655-869e-eeab3edeae6a'
(
    `D` Date,
    `ID` Int64,
    `Ver` UInt64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/tableName/{shard}/', '{replica}', Ver)
PARTITION BY toYYYYMM(D)
ORDER BY ID
SETTINGS index_granularity = 8192
ATTACH TABLE _ UUID 'f438d816-605d-4fe0-a9cb-4edba3ce72dd'
(
    `generation` UInt64,
    `date_key` DateTime,
    `number` UInt64,
    `text` String,
    `expired` DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_table_repl1', '{replica}', ver)
PARTITION BY toMonth(date_key)
ORDER BY (generation, date_key)
SETTINGS index_granularity = 8192
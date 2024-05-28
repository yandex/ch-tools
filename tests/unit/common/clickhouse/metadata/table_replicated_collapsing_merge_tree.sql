ATTACH TABLE _ UUID '9317cb30-1efd-44bd-ab88-d0e3a025965a'
(
    `id` UInt32,
    `value` Int32,
    `sign` Int8
)
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/{shard}/example_replicated_collapsing_mergetree', '{replica}', sign)
ORDER BY id
SETTINGS index_granularity = 8192
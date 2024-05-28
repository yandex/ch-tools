ATTACH TABLE _ UUID '8ac44a5e-091e-4dc4-9eb0-0ba577b3afd7'
(
    `id` UInt32,
    `value` AggregateFunction(sum, UInt32)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/example_replicated_aggregating_mergetree', '{replica}')
ORDER BY id
SETTINGS index_granularity = 8192
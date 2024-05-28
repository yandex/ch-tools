ATTACH TABLE _ UUID '10ccbec1-6b78-48fe-a51a-fb7c9f7fbe4a'
(
    `id` UInt32,
    `value` Int32,
    `sign` Int8,
    `version` UInt32
)
ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{shard}/example_replicated_versioned_collapsing_mergetree', '{replica}', sign, version)
ORDER BY id
SETTINGS index_granularity = 8192
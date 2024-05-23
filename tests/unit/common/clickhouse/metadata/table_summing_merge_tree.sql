ATTACH TABLE _ UUID '5f55555d-c9f7-47ac-8d87-b3ac8a889161'
(
    `key` UInt32,
    `value` UInt32
)
ENGINE = SummingMergeTree
ORDER BY key
SETTINGS index_granularity = 8192
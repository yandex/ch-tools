ATTACH TABLE _ UUID 'c322e832-2628-45f9-b2f5-fd659078c5c2'
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key
SETTINGS index_granularity = 8192
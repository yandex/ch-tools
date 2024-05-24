ATTACH TABLE _ UUID '9612256b-b461-4df5-8015-72f9727d1f95'
(
    `generation` UInt64,
    `date_key` DateTime,
    `number` UInt64,
    `UUID` String,
    `expired` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toMonth(date_key)
ORDER BY (generation, date_key)
TTL expired + toIntervalMinute(3) TO DISK 'object_storage'
SETTINGS index_granularity = 819
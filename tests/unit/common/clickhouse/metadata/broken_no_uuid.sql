ATTACH TABLE _ UUID
(
    `generation` UInt64,
    `date_key` DateTime,
    `number` UInt64,
    `text` String,
    `expired` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toMonth(date_key)
ORDER BY (generation, date_key)
TTL expired + toIntervalMinute(3) TO DISK 'object_storage'
SETTINGS index_granularity = 819
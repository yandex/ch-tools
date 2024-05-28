ATTACH TABLE _ UUID '40c7c9a8-7451-4a10-8b43-443436f33413'
(
    `generation` UInt64,
    `date_key` DateTime,
    `number` UInt64,
    `text` String,
    `expired` DateTime DEFAULT now()
)
ENGINE
PARTITION BY toMonth(date_key)
ORDER BY (generation, date_key)
TTL expired + toIntervalMinute(3) TO DISK 'object_storage'
SETTINGS index_granularity = 819
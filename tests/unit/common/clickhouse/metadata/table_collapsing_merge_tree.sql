ATTACH TABLE _ UUID '122b369e-3866-4c2b-8ca1-9e07c75ecee0'
(
    `UserID` UInt64,
    `PageViews` UInt8,
    `Duration` UInt8,
    `Sign` Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
SETTINGS index_granularity = 8192
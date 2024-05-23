ATTACH TABLE _ UUID '42089e02-c13c-4b52-a1bf-4f9aa3e84e56'
(
    `UserID` UInt64,
    `PageViews` UInt8,
    `Duration` UInt8,
    `Sign` Int8,
    `Version` UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
SETTINGS index_granularity = 8192
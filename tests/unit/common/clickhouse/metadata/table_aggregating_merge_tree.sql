ATTACH TABLE _ UUID '40c7c9a8-7451-4a10-8b43-443436f33413'
(
    `StartDate` DateTime64(3),
    `CounterID` UInt64,
    `Visits` AggregateFunction(sum, Nullable(Int32)),
    `Users` AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree
ORDER BY (StartDate, CounterID)
SETTINGS index_granularity = 8192
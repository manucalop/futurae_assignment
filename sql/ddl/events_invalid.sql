create or replace table events_invalid (
    raw varchar
    , errors varchar []
    , processed_at timestamp
    , processed_by varchar
    , _offset integer primary key
)

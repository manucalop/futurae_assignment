create or replace table events (
    event_id varchar primary key
    , event_ts timestamp
    , service varchar
    , event_type varchar not null
    , latency_ms integer
    , status_code integer
    , user_id varchar
    , raw varchar
    , processed_at timestamp
    , processed_by varchar
)

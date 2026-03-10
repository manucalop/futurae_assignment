create or replace table metrics (
    service varchar not null
    , event_date date not null
    , event_hour integer not null
    , event_minute integer not null
    , request_count integer not null
    , avg_latency_ms double
    , error_rate double
    , processed_at timestamp
    , processed_by varchar
    , primary key (service, event_date, event_hour, event_minute)
)

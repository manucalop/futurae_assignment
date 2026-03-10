begin transaction;
truncate {{ target_table }};
insert into {{ target_table }}
select
    service
    , event_ts::date as event_date
    , hour(event_ts) as event_hour
    , minute(event_ts) as event_minute
    , count(*) as request_count
    , avg(latency_ms) as avg_latency_ms
    , avg(
        case
            when status_code < 200 or status_code > 299
                then 1.0
            else 0.0
        end
    ) as error_rate
    , now() as processed_at
    , '{{ processed_by }}' as processed_by
from {{ source_table }}
where service is not null
group by service, event_date, event_hour, event_minute;
commit;

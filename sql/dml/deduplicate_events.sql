begin transaction;
truncate {{ target_table }};
insert into {{ target_table }}
select
    * exclude (_offset, processed_at, processed_by)
    , now() as processed_at
    , '{{ processed_by }}' as processed_by
from {{ source_table }}
qualify row_number() over (partition by event_id order by _offset) = 1;
commit;

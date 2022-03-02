


create or replace view autopie_ebpdemo.tmp_staging_view_source_extract_0_0000000000 as
select "call_id"::text as "call_id", 
       "call_start_epoch"::bigint as "call_start_epoch", 
       "call_duration"::int as "call_duration", 
       "customer_id"::text as "customer_id", 
       "agent_id"::text as "agent_id"
       from testsatmap.calltable;


create or replace view autopie_ebpdemo.tmp_staging_view_expression_extract_1_0000000000 as
select  (call_id)::text as call_id, 
        (to_timestamp(call_start_epoch))::timestamptz as time_call_start, 
        (to_timestamp(call_start_epoch + call_duration))::timestamptz as time_call_end, 
        (call_duration)::int4 as num_duration_seconds, 
        (trim(customer_id))::text as customer_id, 
        (lower(trim(agent_id)))::text as agent_id
        from autopie_ebpdemo.tmp_staging_view_source_extract_0_0000000000;


drop table if exists autopie_ebpdemo.tmp_staging_fact_0000000000;
create table autopie_ebpdemo.tmp_staging_fact_0000000000 as
(
    with expression_extract_1 as
    (
        select * from autopie_ebpdemo.tmp_staging_view_expression_extract_1_0000000000
    ),
    expression_extract_2 as
    (
        select  t.*,
                (round(num_duration_seconds/60,2))::numeric as num_duration_minutes
                from expression_extract_1 t
    ),
    filter__3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
        where 1=1
        and agent_id is not null
        and customer_id is not null
        and num_duration_seconds > 10
    ),
    incremental_extract__4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter__3
        where time_call_start >= to_date('2020-01-01', 'YYYY-MM-DD')
          and time_call_start  < to_date('2021-01-01', 'YYYY-MM-DD')
    ),
    deduplication__5 as
    (
        select /* deduplicate by strategy */
               call_id
               , max(time_call_start) as time_call_start
               , max(time_call_end) as time_call_end
               , max(num_duration_seconds) as num_duration_seconds
               , max(customer_id) as customer_id
               , max(agent_id) as agent_id
               , max(num_duration_minutes) as num_duration_minutes
        from incremental_extract__4
        group by call_id 
    ),
    coarsening_6 as
    (
        select * from deduplication__5
    )
    select  t.*,
            1 autopie_run_id
    from coarsening_6 t
);

drop view if exists autopie_ebpdemo.tmp_staging_view_source_extract_0_0000000000 cascade;
drop view if exists autopie_ebpdemo.tmp_staging_view_expression_extract_1_0000000000 cascade;


-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
    select 1 from information_schema.tables 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
)
then
    create table autopie_ebpdemo.obj_fact_calls as (select * from autopie_ebpdemo.tmp_staging_fact_0000000000 where 1=0);
    alter  table autopie_ebpdemo.obj_fact_calls add column row_id bigserial not null;
    alter  table autopie_ebpdemo.obj_fact_calls add primary key (call_id, time_call_end);
    --
    create index ixrid_obj_fact_calls on autopie_ebpdemo.obj_fact_calls(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from autopie_ebpdemo.obj_fact_calls where (call_id, time_call_end) in (select call_id, time_call_end from autopie_ebpdemo.tmp_staging_fact_0000000000);
insert into autopie_ebpdemo.obj_fact_calls select * from autopie_ebpdemo.tmp_staging_fact_0000000000;
drop table autopie_ebpdemo.tmp_staging_fact_0000000000;



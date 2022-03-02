create or replace view autopie_ebpdemo.tmp_staging_view_source_extract_0 as
select
    call_id::text as "call_id", 
    call_start_epoch::bigint as "call_start_epoch", 
    call_duration::int as "call_duration", 
    customer_id::text as "customer_id", 
    agent_id::text as "agent_id" 
from testsatmap.calltable
;


create or replace view autopie_ebpdemo.tmp_staging_view_expression_extract_1 as
select
    (call_id)::text as call_id, 
    (to_timestamp(call_start_epoch))::timestamptz as time_call_start, 
    (to_timestamp(call_start_epoch + call_duration))::timestamptz as time_call_end, 
    (call_duration)::int4 as num_duration_seconds, 
    (trim(customer_id))::text as customer_id, 
    (lower(trim(agent_id)))::text as agent_id 
from autopie_ebpdemo.tmp_staging_view_source_extract_0
;

drop table if exists autopie_ebpdemo.tmp_staging_fact_calls;
create table autopie_ebpdemo.tmp_staging_fact_calls as
(
    with expression_extract_1 as
    (
        select * from autopie_ebpdemo.tmp_staging_view_expression_extract_1
    ),
    expression_extract_2 as
    (
        select  t.*,
                (round(num_duration_seconds/60,2))::numeric as num_duration_minutes
        from expression_extract_1 t
    ),
    filter_3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
        where 1=1
        and agent_id is not null
        and customer_id is not null
        and num_duration_seconds > 10
    ),
    incremental_extract_4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter_3
        where time_call_start >= to_date('1900-01-01', 'YYYY-MM-DD')
          and time_call_start  < to_date('2100-01-01', 'YYYY-MM-DD')
    ),
    deduplication_5 as
    (
        select /* deduplicate by strategy */
            call_id
            , max(time_call_start) as time_call_start
            , max(time_call_end) as time_call_end
            , max(num_duration_seconds) as num_duration_seconds
            , max(customer_id) as customer_id
            , max(agent_id) as agent_id
            , max(num_duration_minutes) as num_duration_minutes
        from incremental_extract_4
        group by call_id 
    ),
    coarsening_6 as
    (
        select * from deduplication_5
    )
    select * from coarsening_6
)
;

drop view if exists autopie_ebpdemo.tmp_staging_view_source_extract_0 cascade;
drop view if exists autopie_ebpdemo.tmp_staging_view_expression_extract_1 cascade;

-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
    select 1 from information_schema.tables 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
)
then
    create table autopie_ebpdemo.obj_fact_calls as (select * from autopie_ebpdemo.tmp_staging_fact_calls where 1=0);
    alter  table autopie_ebpdemo.obj_fact_calls add column row_id bigserial not null;
    alter  table autopie_ebpdemo.obj_fact_calls add primary key (call_id, time_call_end);
    --
    create index ixrid_obj_fact_calls on autopie_ebpdemo.obj_fact_calls(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from autopie_ebpdemo.obj_fact_calls where (call_id, time_call_end) in (select call_id, time_call_end from autopie_ebpdemo.tmp_staging_fact_calls);
insert into autopie_ebpdemo.obj_fact_calls select * from autopie_ebpdemo.tmp_staging_fact_calls;
drop table autopie_ebpdemo.tmp_staging_fact_calls;






create or replace view autopie_ebpdemo.tmp_staging_view_source_extract_0 as
select
    outcome_date::date as "outcome_date", 
    customer_id::text as "customer_id", 
    outcome_code::text as "outcome_code" 
from testsatmap.outcomes
;


create or replace view autopie_ebpdemo.tmp_staging_view_expression_extract_1 as
select
    (outcome_date)::date as date_outcome, 
    (trim(customer_id))::text as customer_id, 
    (outcome_code::numeric)::numeric as num_outcome_amount 
from autopie_ebpdemo.tmp_staging_view_source_extract_0
;

drop table if exists autopie_ebpdemo.tmp_staging_fact_outcomes;
create table autopie_ebpdemo.tmp_staging_fact_outcomes as
(
    with expression_extract_1 as
    (
        select * from autopie_ebpdemo.tmp_staging_view_expression_extract_1
    ),
    expression_extract_2 as
    (
        select * from expression_extract_1
    ),
    filter_3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
        where 1=1
        and num_outcome_amount > 0
        and customer_id is not null
    ),
    incremental_extract_4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter_3
        where date_outcome >= to_date('1900-01-01', 'YYYY-MM-DD')
          and date_outcome  < to_date('2100-01-01', 'YYYY-MM-DD')
    ),
    deduplication_5 as
    (
        select /* deduplicate by strategy */
            customer_id, date_outcome, num_outcome_amount
        from incremental_extract_4
        group by customer_id, date_outcome, num_outcome_amount 
    ),
    coarsening_6 as
    (
        select /* flatten all dimensions, fit into EAVT format */
            customer_id
            , date_outcome
            
            , sum(num_outcome_amount) as sum_outcome_amount
            from deduplication_5
        group by customer_id, date_outcome
    )
    select * from coarsening_6
)
;

drop view if exists autopie_ebpdemo.tmp_staging_view_source_extract_0 cascade;
drop view if exists autopie_ebpdemo.tmp_staging_view_expression_extract_1 cascade;

-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
    select 1 from information_schema.tables 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_outcomes'
)
then
    create table autopie_ebpdemo.obj_fact_outcomes as (select * from autopie_ebpdemo.tmp_staging_fact_outcomes where 1=0);
    alter  table autopie_ebpdemo.obj_fact_outcomes add column row_id bigserial not null;
    alter  table autopie_ebpdemo.obj_fact_outcomes add primary key (customer_id, date_outcome);
    --
    create index ixrid_obj_fact_outcomes on autopie_ebpdemo.obj_fact_outcomes(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from autopie_ebpdemo.obj_fact_outcomes where (customer_id, date_outcome) in (select customer_id, date_outcome from autopie_ebpdemo.tmp_staging_fact_outcomes);
insert into autopie_ebpdemo.obj_fact_outcomes select * from autopie_ebpdemo.tmp_staging_fact_outcomes;
drop table autopie_ebpdemo.tmp_staging_fact_outcomes;
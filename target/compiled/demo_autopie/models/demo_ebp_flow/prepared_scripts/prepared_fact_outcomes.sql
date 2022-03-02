drop view if exists testsatmap.tmp_staging_view_source_extract_0 cascade;
create view testsatmap.tmp_staging_view_source_extract_0 as
select
    outcome_date::date as "outcome_date", 
    customer_id::text as "customer_id", 
    outcome_code::text as "outcome_code" 
from testsatmap.outcomes
;


drop view if exists testsatmap.tmp_staging_view_expression_extract_1  cascade;
create view testsatmap.tmp_staging_view_expression_extract_1 as
select
    (outcome_date)::date as date_outcome, 
    (trim(customer_id))::text as customer_id, 
    (outcome_code::numeric)::numeric as num_outcome_amount 
from testsatmap.tmp_staging_view_source_extract_0
;

drop table if exists testsatmap.tmp_staging_fact_outcomes;
create table testsatmap.tmp_staging_fact_outcomes as
(
    with expression_extract_1 as
    (
        select * from testsatmap.tmp_staging_view_expression_extract_1
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

drop view if exists testsatmap.tmp_staging_view_source_extract_0 cascade;
drop view if exists testsatmap.tmp_staging_view_expression_extract_1 cascade;

-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
    select 1 from information_schema.tables 
    where table_schema||'.'|| table_name = 'testsatmap.obj_fact_outcomes'
)
then
    create table testsatmap.obj_fact_outcomes as (select * from testsatmap.tmp_staging_fact_outcomes where 1=0);
    alter  table testsatmap.obj_fact_outcomes add column row_id bigserial not null;
    alter  table testsatmap.obj_fact_outcomes add primary key (customer_id, date_outcome);
    --
    create index ixrid_obj_fact_outcomes on testsatmap.obj_fact_outcomes(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from testsatmap.obj_fact_outcomes where (customer_id, date_outcome) in (select customer_id, date_outcome from testsatmap.tmp_staging_fact_outcomes);
insert into testsatmap.obj_fact_outcomes select * from testsatmap.tmp_staging_fact_outcomes;
drop table testsatmap.tmp_staging_fact_outcomes;
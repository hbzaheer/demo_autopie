


create or replace view autopie_ebpdemo.tmp_staging_view_source_extract_0_0000000000 as
select "outcome_date"::date as "outcome_date", 
       "customer_id"::text as "customer_id", 
       "outcome_code"::text as "outcome_code"
       from testsatmap.outcomes;


create or replace view autopie_ebpdemo.tmp_staging_view_expression_extract_1_0000000000 as
select  (outcome_date)::date as date_outcome, 
        (trim(customer_id))::text as customer_id, 
        (outcome_code::numeric)::numeric as num_outcome_amount
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
        select * from expression_extract_1
    ),
    filter__3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
        where 1=1
        and num_outcome_amount > 0
        and customer_id is not null
    ),
    incremental_extract__4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter__3
        where date_outcome >= to_date('2020-01-01', 'YYYY-MM-DD')
          and date_outcome  < to_date('2021-01-01', 'YYYY-MM-DD')
    ),
    deduplication__5 as
    (
        select /* deduplicate by strategy */
               customer_id, date_outcome, num_outcome_amount
        from incremental_extract__4
        group by customer_id, date_outcome, num_outcome_amount 
    ),
    coarsening_6 as
    (
        select /* flatten all dimensions, fit into EAVT format */
              customer_id
              , date_outcome
              
              , sum(num_outcome_amount) as sum_outcome_amount
              from deduplication__5
        group by customer_id, date_outcome
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
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_outcomes'
)
then
    create table autopie_ebpdemo.obj_fact_outcomes as (select * from autopie_ebpdemo.tmp_staging_fact_0000000000 where 1=0);
    alter  table autopie_ebpdemo.obj_fact_outcomes add column row_id bigserial not null;
    alter  table autopie_ebpdemo.obj_fact_outcomes add primary key (customer_id, date_outcome);
    --
    create index ixrid_obj_fact_outcomes on autopie_ebpdemo.obj_fact_outcomes(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from autopie_ebpdemo.obj_fact_outcomes where (customer_id, date_outcome) in (select customer_id, date_outcome from autopie_ebpdemo.tmp_staging_fact_0000000000);
insert into autopie_ebpdemo.obj_fact_outcomes select * from autopie_ebpdemo.tmp_staging_fact_0000000000;
drop table autopie_ebpdemo.tmp_staging_fact_0000000000;



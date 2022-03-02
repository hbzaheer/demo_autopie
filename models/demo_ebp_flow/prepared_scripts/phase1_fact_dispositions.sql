


create or replace view autopie_ebpdemo.tmp_staging_view_source_extract_0_0000000000 as
select "disposition_epoch"::bigint as "disposition_epoch", 
       "customer_id"::text as "customer_id", 
       "agent_id"::text as "agent_id", 
       "disp_code"::text as "disp_code"
       from testsatmap.calldispositions;


create or replace view autopie_ebpdemo.tmp_staging_view_expression_extract_1_0000000000 as
select  (to_timestamp(disposition_epoch))::timestamptz as time_disposition_made, 
        (trim(customer_id))::text as customer_id, 
        (lower(trim(agent_id)))::text as agent_id, 
        (lower(trim(disp_code)))::text as cat_disposition
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
        and agent_id is not null
        and customer_id is not null
        and cat_disposition in ('k', 'm', 'a')
    ),
    incremental_extract__4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter__3
        where time_disposition_made >= to_date('2020-01-01', 'YYYY-MM-DD')
          and time_disposition_made  < to_date('2021-01-01', 'YYYY-MM-DD')
    ),
    deduplication__5 as
    (
        select /* deduplicate by strategy */
               agent_id, cat_disposition, time_disposition_made
               , max(customer_id) as customer_id
        from incremental_extract__4
        group by agent_id, cat_disposition, time_disposition_made 
    ),
    coarsening_6 as
    (
        select /* flatten all dimensions, fit into EAVT format */
              agent_id
              , time_disposition_made
              
              , max(customer_id) as customer_id
              , sum(case when cat_disposition = 'k' then 1 else 0 end) as flg_k_disp_made
              , sum(case when cat_disposition = 'm' then 1 else 0 end) as flg_m_disp_made
              , sum(case when cat_disposition = 'a' then 1 else 0 end) as flg_a_disp_made
              from deduplication__5
        group by agent_id, time_disposition_made
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
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_dispositions'
)
then
    create table autopie_ebpdemo.obj_fact_dispositions as (select * from autopie_ebpdemo.tmp_staging_fact_0000000000 where 1=0);
    alter  table autopie_ebpdemo.obj_fact_dispositions add column row_id bigserial not null;
    alter  table autopie_ebpdemo.obj_fact_dispositions add primary key (agent_id, time_disposition_made);
    --
    create index ixrid_obj_fact_dispositions on autopie_ebpdemo.obj_fact_dispositions(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from autopie_ebpdemo.obj_fact_dispositions where (agent_id, time_disposition_made) in (select agent_id, time_disposition_made from autopie_ebpdemo.tmp_staging_fact_0000000000);
insert into autopie_ebpdemo.obj_fact_dispositions select * from autopie_ebpdemo.tmp_staging_fact_0000000000;
drop table autopie_ebpdemo.tmp_staging_fact_0000000000;



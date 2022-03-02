


create or replace view autopie_ebpdemo.tmp_staging_view_source_extract_0_0000000000 as
select "data_date"::date as "data_date", 
       "customer_id"::text as "customer_id", 
       "customer_cat_attrib"::text as "customer_cat_attrib", 
       "customer_num_attrib"::numeric as "customer_num_attrib"
       from testsatmap.crm;


create or replace view autopie_ebpdemo.tmp_staging_view_expression_extract_1_0000000000 as
select  (data_date)::date as date_data, 
        (trim(customer_id))::text as customer_id, 
        (customer_num_attrib)::numeric as num_attrib, 
        (customer_cat_attrib)::text as cat_attrib
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
        and customer_id is not null
    ),
    incremental_extract__4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter__3
        where date_data >= to_date('2020-01-01', 'YYYY-MM-DD')
          and date_data  < to_date('2021-01-01', 'YYYY-MM-DD')
    ),
    deduplication__5 as
    (
        select /* deduplicate by strategy */
               customer_id, date_data
               , max(num_attrib) as num_attrib
               , max(cat_attrib) as cat_attrib
        from incremental_extract__4
        group by customer_id, date_data 
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
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_crm'
)
then
    create table autopie_ebpdemo.obj_fact_crm as (select * from autopie_ebpdemo.tmp_staging_fact_0000000000 where 1=0);
    alter  table autopie_ebpdemo.obj_fact_crm add column row_id bigserial not null;
    alter  table autopie_ebpdemo.obj_fact_crm add primary key (customer_id, date_data);
    --
    create index ixrid_obj_fact_crm on autopie_ebpdemo.obj_fact_crm(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from autopie_ebpdemo.obj_fact_crm where (customer_id, date_data) in (select customer_id, date_data from autopie_ebpdemo.tmp_staging_fact_0000000000);
insert into autopie_ebpdemo.obj_fact_crm select * from autopie_ebpdemo.tmp_staging_fact_0000000000;
drop table autopie_ebpdemo.tmp_staging_fact_0000000000;



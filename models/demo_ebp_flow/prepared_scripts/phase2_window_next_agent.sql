



drop table if exists autopie_ebpdemo.tmp_alter_staging_0000000000;
create table autopie_ebpdemo.tmp_alter_staging_0000000000 as
(
  select  row_id,
          /* calculate window functions on whole fact table - TODO, will optimize this */
          lead(time_call_start) over (partition by agent_id order by time_call_start) as time_agent_next_call, 
          lag(time_call_start) over (partition by agent_id order by time_call_start) as time_agent_prev_call
  from autopie_ebpdemo.obj_fact_calls
);

alter table autopie_ebpdemo.tmp_alter_staging_0000000000 add primary key (row_id);

-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
  select 1 from information_schema.columns 
  where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
  and column_name = 'time_agent_next_call'
)
then
  alter table autopie_ebpdemo.obj_fact_calls add column time_agent_next_call timestamp;
end if;
end
$$;
do $$                  
begin
if not exists
( 
  select 1 from information_schema.columns 
  where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
  and column_name = 'time_agent_prev_call'
)
then
  alter table autopie_ebpdemo.obj_fact_calls add column time_agent_prev_call timestamp;
end if;
end
$$;
update autopie_ebpdemo.obj_fact_calls as t
set
    time_agent_next_call = m.time_agent_next_call,
    time_agent_prev_call = m.time_agent_prev_call,
    autopie_run_id = 1
from autopie_ebpdemo.tmp_alter_staging_0000000000 m 
where t.row_id = m.row_id;

drop table autopie_ebpdemo.tmp_alter_staging_0000000000;



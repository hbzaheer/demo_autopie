













drop table if exists autopie_ebpdemo.tmp_target_affected_0000000000;
create table autopie_ebpdemo.tmp_target_affected_0000000000 as
(
    select t.row_id
    from autopie_ebpdemo.obj_fact_calls t
    where exists
    ( 
        select 1
        from autopie_ebpdemo.obj_fact_outcomes s
        where s.autopie_run_id = 1
        and s.customer_id = t.customer_id
        and s.date_outcome >= date(t.time_call_start)
    )
);

alter table autopie_ebpdemo.tmp_target_affected_0000000000 add primary key (row_id);

drop table if exists autopie_ebpdemo.tmp_pre_join_0000000000;
create table autopie_ebpdemo.tmp_pre_join_0000000000 as
(
    with compute_preference_scoring as
    (
        -- for each source scan potential targets, pick one by preference
        select  s.row_id s_row_id, t.row_id t_row_id,
                -- scores, bigger is more preferred
                (date(t.time_call_start) - s.date_outcome)::int as pref_score_1,
                (date(t.time_call_start) - t.time_call_start)::interval as pref_score_2,
                -- attributes to be propagated
                s.sum_outcome_amount
        from autopie_ebpdemo.obj_fact_calls t
        join autopie_ebpdemo.tmp_target_affected_0000000000 a
        on t.row_id = a.row_id
        join autopie_ebpdemo.obj_fact_outcomes s 
        on 1=1
        and s.customer_id = t.customer_id
        and s.date_outcome >= date(t.time_call_start)    
    ),
    pick_best_target as
    (
        select  s_row_id,
                -- collapse source expressions arbitrarily
                max(sum_outcome_amount) as sum_outcome_amount, 
                max(pref_score_1) as pref_score_1, 
                max(pref_score_2) as pref_score_2,
                -- prefer best targets id
                (array_agg(t_row_id order by  pref_score_1 desc, pref_score_2 desc))[1] t_row_id
        from compute_preference_scoring
        group by s_row_id
    ),
    target_collapse as
    (
		select  t_row_id
				, sum(sum_outcome_amount) as sum_outcome_amount
		from pick_best_target
		group by t_row_id       
    )
    select * from target_collapse
);

alter table autopie_ebpdemo.tmp_pre_join_0000000000 add primary key (t_row_id);

-- this should be done only once if new slot is missing

do $$                  
begin
if not exists
( 
    select 1 from information_schema.columns 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
    and column_name = 'sum_outcome_amount'
)
then
    alter table autopie_ebpdemo.obj_fact_calls add column sum_outcome_amount numeric;
end if;
end
$$;



update autopie_ebpdemo.obj_fact_calls as t
set
    sum_outcome_amount = m.sum_outcome_amount, 
    autopie_run_id = 1
from autopie_ebpdemo.tmp_pre_join_0000000000 m 
where t.row_id = m.t_row_id;

drop table if exists autopie_ebpdemo.tmp_pre_join_0000000000;
drop table if exists autopie_ebpdemo.tmp_target_affected_0000000000;



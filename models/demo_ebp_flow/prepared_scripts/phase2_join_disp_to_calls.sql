













drop table if exists autopie_ebpdemo.tmp_target_affected_0000000000;
create table autopie_ebpdemo.tmp_target_affected_0000000000 as
(
    select t.row_id
    from autopie_ebpdemo.obj_fact_calls t
    where exists
    ( 
        select 1
        from autopie_ebpdemo.obj_fact_dispositions s
        where s.autopie_run_id = 1
        and s.agent_id = t.agent_id
        and s.customer_id = t.customer_id
        and date(s.time_disposition_made) = date(t.time_call_start)
        and s.time_disposition_made >= t.time_call_start - interval '120 second'
        and s.time_disposition_made <= t.time_call_end + interval '120 second'
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
                case when s.time_disposition_made between t.time_call_start and t.time_call_end then 1 else 0 end as pref_score_1,
                extract(epoch from greatest(s.time_disposition_made - t.time_call_end, t.time_call_start - s.time_disposition_made))::int as pref_score_2,
                -- attributes to be propagated
                s.flg_k_disp_made,
                s.flg_m_disp_made,
                s.flg_a_disp_made,
                s.time_disposition_made
        from autopie_ebpdemo.obj_fact_calls t
        join autopie_ebpdemo.tmp_target_affected_0000000000 a
        on t.row_id = a.row_id
        join autopie_ebpdemo.obj_fact_dispositions s 
        on 1=1
        and s.agent_id = t.agent_id
        and s.customer_id = t.customer_id
        and date(s.time_disposition_made) = date(t.time_call_start)
        and s.time_disposition_made >= t.time_call_start - interval '120 second'
        and s.time_disposition_made <= t.time_call_end + interval '120 second'    
    ),
    pick_best_target as
    (
        select  s_row_id,
                -- collapse source expressions arbitrarily
                max(flg_k_disp_made) as flg_k_disp_made,
                max(flg_m_disp_made) as flg_m_disp_made,
                max(flg_a_disp_made) as flg_a_disp_made,
                max(time_disposition_made) as time_disposition_made, 
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
				, max(flg_k_disp_made) as flg_k_disp_made
				, max(flg_m_disp_made) as flg_m_disp_made
				, max(flg_a_disp_made) as flg_a_disp_made
				, min(time_disposition_made) as time_first_disposition_made
				, max(time_disposition_made) as time_last_disposition_made
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
    and column_name = 'flg_k_disp_made'
)
then
    alter table autopie_ebpdemo.obj_fact_calls add column flg_k_disp_made bigint;
end if;
end
$$;


do $$                  
begin
if not exists
( 
    select 1 from information_schema.columns 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
    and column_name = 'flg_m_disp_made'
)
then
    alter table autopie_ebpdemo.obj_fact_calls add column flg_m_disp_made bigint;
end if;
end
$$;


do $$                  
begin
if not exists
( 
    select 1 from information_schema.columns 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
    and column_name = 'flg_a_disp_made'
)
then
    alter table autopie_ebpdemo.obj_fact_calls add column flg_a_disp_made bigint;
end if;
end
$$;


do $$                  
begin
if not exists
( 
    select 1 from information_schema.columns 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
    and column_name = 'time_first_disposition_made'
)
then
    alter table autopie_ebpdemo.obj_fact_calls add column time_first_disposition_made timestamp;
end if;
end
$$;


do $$                  
begin
if not exists
( 
    select 1 from information_schema.columns 
    where table_schema||'.'|| table_name = 'autopie_ebpdemo.obj_fact_calls'
    and column_name = 'time_last_disposition_made'
)
then
    alter table autopie_ebpdemo.obj_fact_calls add column time_last_disposition_made timestamp;
end if;
end
$$;



update autopie_ebpdemo.obj_fact_calls as t
set
    flg_k_disp_made = m.flg_k_disp_made,
    flg_m_disp_made = m.flg_m_disp_made,
    flg_a_disp_made = m.flg_a_disp_made,
    time_first_disposition_made = m.time_first_disposition_made,
    time_last_disposition_made = m.time_last_disposition_made, 
    autopie_run_id = 1
from autopie_ebpdemo.tmp_pre_join_0000000000 m 
where t.row_id = m.t_row_id;

drop table if exists autopie_ebpdemo.tmp_pre_join_0000000000;
drop table if exists autopie_ebpdemo.tmp_target_affected_0000000000;



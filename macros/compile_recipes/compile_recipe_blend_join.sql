{% macro compile_recipe_blend_join(autopie_prefix, ingredients, target_fact, source_fact) %}

{% set run_object_id = range(10000, 99999) | random ~ range(10000, 99999) | random -%}
{% if env_var('DETERMINISTIC_RUN_ID', 'FALSE') == 'TRUE' %}{% set run_object_id = '0000000000' %}{% endif -%}

{% set autopie_run_id = env_var('AUTOPIE_RUN_ID', '0') -%}

{% set dtype_setup %}
drop view if exists {{autopie_prefix}}tmp_dtype_view_base_{{run_object_id}} cascade;
create or replace view {{autopie_prefix}}tmp_dtype_view_base_{{run_object_id}} as
select  -- attributes to be propagated
        {%- for exp in ingredients['matched_expressions'] %}
        {{exp['expression']}}{{" as "~exp['alias'] if exp['alias']|length>0 else ""}}{{ "," if not loop.last else "" -}}
        {%- endfor %}
from {{autopie_prefix}}obj_fact_{{target_fact}} t
join {{autopie_prefix}}obj_fact_{{source_fact}} s on 1=0;

{%- if ingredients['at_target_choose_sources']|lower == 'collapse' %}
drop view if exists {{autopie_prefix}}tmp_dtype_view_collapse_{{run_object_id}} cascade;
create or replace view {{autopie_prefix}}tmp_dtype_view_collapse_{{run_object_id}} as
select  {%- for exp in ingredients['target_collapse_aggregations'] %}
		{{exp['expression']}}{{" as "~exp['alias'] if exp['alias']|length>0 else ""}}{{ "," if not loop.last else "" -}}
		{%- endfor %}
from {{autopie_prefix}}tmp_dtype_view_base_{{run_object_id}}
{%- endif %}
{% endset %}

{% set dtype_cleanup %}
drop view if exists {{autopie_prefix}}tmp_dtype_view_base_{{run_object_id}} cascade;
drop view if exists {{autopie_prefix}}tmp_dtype_view_collapse_{{run_object_id}} cascade;
{% endset %}

{% set dtype_query %}
select column_name alias, 
       case when replace(udt_name, '_','') = 'bit' then 'int'
            when replace(udt_name, '_','') like 'int8%' then 'bigint'
            when replace(udt_name, '_','') like 'int4%' then 'int'
            when replace(udt_name, '_','') like 'int%' then 'smallint'
            when replace(udt_name, '_','') like 'bool%' then 'int'
            when replace(udt_name, '_','') like 'float%' then 'numeric'
            when replace(udt_name, '_','') like 'float%' then 'numeric'
            when replace(udt_name, '_','') like 'byte%' then 'text'
            when replace(udt_name, '_','') like 'varchar%' then 'text'
            when replace(udt_name, '_','') like 'timestamp%' then 'timestamp'
            else replace(udt_name, '_','') end data_type
from information_schema.columns
where table_schema||'.'||table_name =
{% endset %}

{% set dtype_query_end_pre %}
'{{autopie_prefix}}tmp_dtype_view_base_{{run_object_id}}'
order by ordinal_position;
{% endset %}

{% set dtype_query_end_agg %}
{%- if ingredients['at_target_choose_sources']|lower == 'collapse' %}
'{{autopie_prefix}}tmp_dtype_view_collapse_{{run_object_id}}'
{%- else %}
'{{autopie_prefix}}tmp_dtype_view_base_{{run_object_id}}'
{%- endif %}
order by ordinal_position;
{% endset %}

{% do run_query(dtype_setup) -%}
{% set dtype_obtain_pre = run_query(dtype_query~dtype_query_end_pre) -%}
{% set dtype_obtain_agg = run_query(dtype_query~dtype_query_end_agg) -%}
{% do run_query(dtype_cleanup) -%}

{% set sql -%}
drop table if exists {{autopie_prefix}}tmp_target_affected_{{run_object_id}};
create table {{autopie_prefix}}tmp_target_affected_{{run_object_id}} as
(
    select t.row_id
    from {{autopie_prefix}}obj_fact_{{target_fact}} t
    where exists
    ( 
        select 1
        from {{autopie_prefix}}obj_fact_{{source_fact}} s
        where s.autopie_run_id = {{autopie_run_id}}
        {%- for exp in ingredients['conjunctions'] %}
        and {{exp}}
        {%- endfor %}
    )
);

alter table {{autopie_prefix}}tmp_target_affected_{{run_object_id}} add primary key (row_id);

drop table if exists {{autopie_prefix}}tmp_pre_join_{{run_object_id}};
create table {{autopie_prefix}}tmp_pre_join_{{run_object_id}} as
(
    with compute_preference_scoring as
    (
        -- for each source scan potential targets, pick one by preference
        select  s.row_id s_row_id, t.row_id t_row_id,
                -- scores, bigger is more preferred
                {%- for exp in ingredients['match_preferences'] %}
                {{exp}} as pref_score_{{loop.index}},
                {%- endfor %}
                -- attributes to be propagated
                {%- for exp in ingredients['matched_expressions'] %}
                {{exp['expression']}}{{" as "~exp['alias'] if exp['alias']|length>0 else ""}}{{ "," if not loop.last else "" -}}
                {%- endfor %}
        from {{autopie_prefix}}obj_fact_{{target_fact}} t
        join {{autopie_prefix}}tmp_target_affected_{{run_object_id}} a
        on t.row_id = a.row_id
        join {{autopie_prefix}}obj_fact_{{source_fact}} s 
        on 1=1
        {%- for exp in ingredients['conjunctions'] %}
        and {{exp}}
        {%- endfor %}    
    ),
    {%- if ingredients['from_source_to_target_mapping']|lower == 'best' %}
    pick_best_target as
    (
        select  s_row_id,
                -- collapse source expressions arbitrarily
                {%- for exp in dtype_obtain_pre.rows  %}
                max({{exp['alias']}}) as {{exp['alias']}},
                {%- endfor %}
                {%- for exp in ingredients['match_preferences'] %} 
                max(pref_score_{{loop.index}}) as pref_score_{{loop.index}},
                {%- endfor %}
                -- prefer best targets id
                (array_agg(t_row_id order by {% for exp in ingredients['match_preferences'] %} pref_score_{{loop.index}} desc{{ "," if not loop.last else "" -}}{%- endfor %}))[1] t_row_id
        from compute_preference_scoring
        group by s_row_id
    ),
    {%- else %}
    pick_best_target as
    (
        select * from compute_preference_scoring
    ),
    {%- endif -%}
    {%- if ingredients['at_target_choose_sources']|lower == 'collapse' %}
    target_collapse as
    (
		select  t_row_id
				{%- for exp in ingredients['target_collapse_aggregations'] %}
				, {{exp['expression']}}{{" as "~exp['alias'] if exp['alias']|length>0 else ""}}
		        {%- endfor %}
		from pick_best_target
		group by t_row_id       
    )
    {%- elif ingredients['at_target_choose_sources']|lower == 'collapse_any' %}
    target_collapse as
    (
		select  t_row_id
				{%- for exp in dtype_obtain_pre.rows %}
				, max({{exp['alias']}}){{" as "~exp['alias'] if exp['alias']|length>0 else ""}}
		        {%- endfor %}
		from pick_best_target
		group by t_row_id       
    )
    {%- elif ingredients['at_target_choose_sources']|lower == 'best' %}
    target_collapse as
    (
        select  t_row_id
				{%- for exp in dtype_obtain_pre.rows %}
                , (array_agg({{exp['alias']}} order by {% for exp in ingredients['match_preferences'] %} pref_score_{{loop.index}} desc{{ "," if not loop.last else "" -}}{%- endfor %}))[1] {{exp['alias']}}
		        {%- endfor %}
		from pick_best_target
		group by t_row_id    
    )
    {%- else %}
    target_collapse as
    (
        select * from pick_best_target
    )
    {%- endif %}
    select * from target_collapse
);

alter table {{autopie_prefix}}tmp_pre_join_{{run_object_id}} add primary key (t_row_id);

-- this should be done only once if new slot is missing
{% for exp in dtype_obtain_agg.rows %}
do $$                  
begin
if not exists
( 
    select 1 from information_schema.columns 
    where table_schema||'.'|| table_name = '{{autopie_prefix}}obj_fact_{{target_fact}}'
    and column_name = '{{exp['alias']}}'
)
then
    alter table {{autopie_prefix}}obj_fact_{{target_fact}} add column {{exp['alias']}} {{exp['data_type']}};
end if;
end
$$;

{% endfor %}

update {{autopie_prefix}}obj_fact_{{target_fact}} as t
set 
	{%- for exp in dtype_obtain_agg.rows %}
    {{exp['alias']}} = m.{{exp['alias']}},
	{%- endfor %} 
    autopie_run_id = {{autopie_run_id}}
from {{autopie_prefix}}tmp_pre_join_{{run_object_id}} m 
where t.row_id = m.t_row_id;

drop table if exists {{autopie_prefix}}tmp_pre_join_{{run_object_id}};
drop table if exists {{autopie_prefix}}tmp_target_affected_{{run_object_id}};
{%- endset %}

{# /* return the SQL back to caller */ #}
{{sql}}

{% endmacro %}

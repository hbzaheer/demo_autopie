{% macro compile_recipe_chop_rollup(autopie_prefix, ingredients, target_fact) %}

{% set run_object_id = range(10000, 99999) | random ~ range(10000, 99999) | random -%}
{% if env_var('DETERMINISTIC_RUN_ID', 'FALSE') == 'TRUE' %}{% set run_object_id = '0000000000' %}{% endif -%}

{% set autopie_run_id = env_var('AUTOPIE_RUN_ID', '0') -%}

{% set sql -%}
drop table if exists {{autopie_prefix}}tmp_target_affected_{{run_object_id}};

create table {{autopie_prefix}}tmp_target_affected_{{run_object_id}} as
(
  select s.row_id
  from {{autopie_prefix}}obj_fact_{{ingredients['source_fact']}} s
  where ({{ingredients['entity']}}, {{ingredients['date_expression']}}{{", "~ingredients['expire_expression'] if ingredients['expire_expression']|length>0 else ""}}) in
  (
    select distinct {{ingredients['entity']}}, {{ingredients['date_expression']}}{{", "~ingredients['expire_expression'] if ingredients['expire_expression']|length>0 else ""}} date_call
    from {{autopie_prefix}}obj_fact_{{ingredients['source_fact']}} s
    where s.autopie_run_id = {{autopie_run_id}}
  )
);

alter table {{autopie_prefix}}tmp_target_affected_{{run_object_id}} add primary key (row_id);

drop table if exists {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}};

create table {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}} as
(
    with rollup_1 as
    (
        select {{ingredients['entity']}}, {{ingredients['date_expression']}} as {{ingredients['date_alias']}}, {{ingredients['expire_expression']~" as "~ingredients['expire_alias']~"," if ingredients['expire_expression']|length>0 else ""}}
               {%- for exp in ingredients['aggregations'] %}
               {{exp['expression']}} as {{exp['alias']}}{{ ", " if not loop.last else "" }}
               {%- endfor %}
        from {{autopie_prefix}}obj_fact_{{ingredients['source_fact']}} s
        join {{autopie_prefix}}tmp_target_affected_{{run_object_id}} a
        on a.row_id = s.row_id
        group by {{ingredients['entity']}}, {{ingredients['date_expression']}}{{", "~ingredients['expire_expression']~"," if ingredients['expire_expression']|length>0 else ""}}
        {% if ingredients['having_filter'] | length > 0 -%}
        having 1=1
        {%- for exp in ingredients['having_filter'] %}
        and {{exp['filter_expression']}}
        {%- endfor %}
        {% endif -%}
    ),
    {% if ingredients['expression_extract'] | length > 0 -%}
    expression_extract_2 as
    (
        select t.*
               {% for expression_block in ingredients['expression_extract']['expression_list'] -%}
               ,({{expression_block['expression']}}){{ "::"~expression_block['data_type'] if expression_block['data_type']|length>0 else "" }}{{ " as "~expression_block['alias'] if expression_block['alias']|length>0 else " as "~expression_block['expression'] }}
               {% endfor -%}
        from rollup_1 t
        where 1=1
    ),
    {% else -%}
    expression_extract_2 as
    (
        select * from rollup_1
    ),
    {% endif -%}
    {% if ingredients['filter'] | length > 0 -%}
    filter_3 as
    (
        select /* filtering irrelevant records out */ *
        from uexpression_extract_2
        where 1=1
        {%- for exp in ingredients['filter']['conjunctions'] %}
        and {{exp['filter_expression']}}
        {%- endfor %}
    )
    {% else -%}
    filter_3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
    )
    {% endif -%}
    select r.*, 
           {{autopie_run_id}} autopie_run_id
    from filter_3 r
);

alter table {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}} add primary key ({{ingredients['entity']}}, {{ingredients['date_alias']}}{{", "~ingredients['expire_alias'] if ingredients['expire_expression']|length>0 else ""}});

-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
    select 1 from information_schema.tables 
    where table_schema||'.'|| table_name = '{{autopie_prefix}}obj_fact_{{target_fact['id']}}'
)
then
  create table {{autopie_prefix}}obj_fact_{{target_fact['id']}} as (select * from {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}} where 1=0);
  alter  table {{autopie_prefix}}obj_fact_{{target_fact['id']}} add column row_id bigserial not null;
  alter  table {{autopie_prefix}}obj_fact_{{target_fact['id']}} add primary key ({{ingredients['entity']}}, {{ingredients['date_alias']}}{{", "~ingredients['expire_alias'] if ingredients['expire_expression']|length>0 else ""}});
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from {{autopie_prefix}}obj_fact_{{target_fact['id']}} where ({{ingredients['entity']}}, {{ingredients['date_alias']}}{{", "~ingredients['expire_alias'] if ingredients['expire_expression']|length>0 else ""}}) in (select {{ingredients['entity']}}, {{ingredients['date_alias']}}{{", "~ingredients['expire_alias'] if ingredients['expire_expression']|length>0 else ""}} from {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}});
insert into {{autopie_prefix}}obj_fact_{{target_fact['id']}} select * from {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}};

drop table {{autopie_prefix}}tmp_pre_rollup_{{run_object_id}};
drop table {{autopie_prefix}}tmp_target_affected_{{run_object_id}};

{%- endset %}

{# /* return the SQL back to caller */ #}
{{sql}}

{% endmacro %}

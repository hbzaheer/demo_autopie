{% macro compile_recipe_collect_union(autopie_prefix, ingredients, target_fact) %}

{% set run_object_id = range(10000, 99999) | random ~ range(10000, 99999) | random -%}
{% set params = namespace(aggregations=[], alias="") -%}
{% if env_var('DETERMINISTIC_RUN_ID', 'FALSE') == 'TRUE' %}{% set run_object_id = '0000000000' %}{% endif -%}

{% set autopie_run_id = env_var('AUTOPIE_RUN_ID', '0') -%}

{% set sql -%}
drop table if exists {{autopie_prefix}}tmp_staging_fact_{{run_object_id}};
create table {{autopie_prefix}}tmp_staging_fact_{{run_object_id}} as
(
    with
    {% for union_block in ingredients['nested_union_all_list'] -%}
    union_block_{{loop.index}} as
    (
        select
        {% for expression_block in union_block['expression_list'] -%}
        ({{expression_block['expression']}}){{ "::"~expression_block['data_type'] if expression_block['data_type']|length>0 else "" }}{{ " as "~expression_block['alias'] if expression_block['alias']|length>0 else " as "~expression_block['expression'] }}{{ "," if not loop.last else "" }}
        {% endfor -%}
        from {{autopie_prefix}}obj_fact_{{union_block['source_fact']}} s
        where s.autopie_run_id = {{autopie_run_id}}
    ),
    {% endfor -%}
    unifier__{{run_object_id}} as
    (
        {% for union_block in ingredients['nested_union_all_list'] -%}
        select * from union_block_{{loop.index}}{{ " union all" if not loop.last else "" }}
        {% endfor %}
    ),
    {% if ingredients['filter'] | length > 0 -%}
    filter__{{run_object_id}} as
    (
        select /* filtering irrelevant records out */ *
        from unifier__{{run_object_id}}
        where 1=1
        {%- for exp in ingredients['filter']['conjunctions'] %}
        and {{exp['filter_expression']}}
        {%- endfor %}
    ),
    {% else -%}
    filter__{{run_object_id}} as
    (
        select /* filtering irrelevant records out */ *
        from unifier__{{run_object_id}}
    ),
    {% endif -%}
    {% if ingredients['deduplicate'] | length > 0 -%}
    deduplicate__{{run_object_id}} as
    (
        select /* deduplicate by strategy */
           {{ingredients['deduplicate']['grouping_key']}}
           {% for exp in ingredients['nested_union_all_list'][0]['expression_list'] -%}
           {% set params.alias = exp['alias'] if exp['alias']|length>0 else exp['expression']  -%}
           {%- if params.alias not in ingredients['deduplicate']['grouping_key'].split(',')|string %}
           {%- do params.aggregations.append(params.alias)%}
           {%- endif -%}
           {%- endfor %}
           {%- for exp in params.aggregations %}
           {%- if ingredients['deduplicate']['preference']|lower == 'arbitrary' %}
           , max({{exp}}) as {{exp}}
           {%- else %}
           , (array_agg({{exp}} order by {{ingredients['deduplicate']['preference']}}))[1] as {{exp}}
           {%- endif -%}
           {%- endfor %}
    from filter__{{run_object_id}}
    group by {{ingredients['deduplicate']['grouping_key']}}
    )
    {% else -%}
    deduplicate__{{run_object_id}} as
    (
        select /* filtering irrelevant records out */ *
        from filter__{{run_object_id}}
    )
    {% endif -%}
    select {{autopie_run_id}} autopie_run_id,
           t.* from deduplicate__{{run_object_id}} t
);

-- check if not exists already, this should run once
do $$                  
begin
if not exists
( 
    select 1 from information_schema.tables 
    where table_schema||'.'|| table_name = '{{autopie_prefix}}obj_fact_{{target_fact['id']}}'
)
then
    create table {{autopie_prefix}}obj_fact_{{target_fact['id']}} as (select * from {{autopie_prefix}}tmp_staging_fact_{{run_object_id}} where 1=0);
    alter  table {{autopie_prefix}}obj_fact_{{target_fact['id']}} add column row_id bigserial not null;
    alter  table {{autopie_prefix}}obj_fact_{{target_fact['id']}} add primary key ({{target_fact['entity']}}, {{target_fact['date']}}{{ ", "~target_fact['expire'] if target_fact['expire']|length>0 else "" }});
    --
    create index ixrid_obj_fact_{{target_fact['id']}} on {{autopie_prefix}}obj_fact_{{target_fact['id']}}(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from {{autopie_prefix}}obj_fact_{{target_fact['id']}} where ({{target_fact['entity']}}, {{target_fact['date']}}{{ ", "~target_fact['expire'] if target_fact['expire']|length>0 else "" }}) in (select {{target_fact['entity']}}, {{target_fact['date']}}{{ ", "~target_fact['expire'] if target_fact['expire']|length>0 else "" }} from {{autopie_prefix}}tmp_staging_fact_{{run_object_id}});
insert into {{autopie_prefix}}obj_fact_{{target_fact['id']}} select * from {{autopie_prefix}}tmp_staging_fact_{{run_object_id}};
drop table {{autopie_prefix}}tmp_staging_fact_{{run_object_id}};

{%- endset %}

{# /* return the SQL back to caller */ #}
{{sql}}

{% endmacro %}

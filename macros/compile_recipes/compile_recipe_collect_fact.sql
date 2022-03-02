{% macro compile_recipe_collect_fact(autopie_prefix, ingredients, target_fact) %}
{% set params = namespace(aggregations=[], expressions=[]) -%}
{% set run_object_id = range(10000, 99999) | random ~ range(10000, 99999) | random -%}
{% if env_var('DETERMINISTIC_RUN_ID', 'FALSE') == 'TRUE' %}{% set run_object_id = '0000000000' %}{% endif -%}

{% set autopie_run_id = env_var('AUTOPIE_RUN_ID', '0') -%}
{% set autopie_run_start_date = env_var('AUTOPIE_RUN_START_DATE', '1900-01-01') -%}
{% set autopie_run_end_date = env_var('AUTOPIE_RUN_END_DATE', '2100-01-01') -%}

{% set obtain_types -%}
select '"'||column_name||'"::'||
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
            else replace(udt_name, '_','') end ||
       ' as "'||column_name||'"' select_text
from information_schema.columns 
where table_schema||'.'||table_name = '{{ingredients['expression_extract']['source_raw']}}'
order by ordinal_position ;
{% endset -%}

{% set pre_extract_sql -%}
{% set data_type = run_query(obtain_types) -%}
create or replace view {{autopie_prefix}}tmp_staging_view_source_extract_0_{{run_object_id}} as
select {% for dstring in data_type.rows -%}
       {{dstring['select_text']}}{{ ", " if not loop.last else "" }}
       {% endfor -%} 
from {{ingredients['expression_extract']['source_raw']}};
{% endset -%}

{% do run_query(pre_extract_sql) -%}

{% set pre_view_sql -%}
create or replace view {{autopie_prefix}}tmp_staging_view_expression_extract_1_{{run_object_id}} as
select  {% for exp in ingredients['expression_extract']['expression_list'] -%}
        {% do run_query("drop view if exists "~autopie_prefix~"tmp_exp_type_"~run_object_id~" cascade") -%}
        {% do run_query("create or replace view "~autopie_prefix~"tmp_exp_type_"~run_object_id~" as select "~exp['expression']~" exp from "~autopie_prefix~"tmp_staging_view_source_extract_0_"~run_object_id~" limit 1") -%}
        {% set data_type = run_query("select replace(udt_name, '_','') dt from information_schema.columns where table_schema||'.'||table_name = '"~autopie_prefix~"tmp_exp_type_"~run_object_id~"' and column_name = 'exp'") -%}
        {% do params.expressions.append(exp['alias']) -%}
        ({{exp['expression']}})::{{data_type.rows[0]['dt']}} as {{exp['alias']}}{{ ", " if not loop.last else "" }}
        {% do run_query("drop view if exists "~autopie_prefix~"tmp_exp_type_"~run_object_id~" cascade") -%}
        {% endfor -%} 
from {{autopie_prefix}}tmp_staging_view_source_extract_0_{{run_object_id}};
{% endset -%}

{% do run_query(pre_view_sql) -%}

{% set clean_intermediate -%}
drop view if exists {{autopie_prefix}}tmp_staging_view_source_extract_0_{{run_object_id}} cascade;
drop view if exists {{autopie_prefix}}tmp_staging_view_expression_extract_1_{{run_object_id}} cascade;
{% endset -%}

{% set sql -%}
{{pre_extract_sql}}

{{pre_view_sql}}

drop table if exists {{autopie_prefix}}tmp_staging_fact_{{run_object_id}};
create table {{autopie_prefix}}tmp_staging_fact_{{run_object_id}} as
(
    with expression_extract_1 as
    (
        select * from {{autopie_prefix}}tmp_staging_view_expression_extract_1_{{run_object_id}}
    ),
    {% if ingredients['expression_extract_outer'] | length > 0 -%}
    expression_extract_2 as
    (
        select  t.*,
                {% for exp in ingredients['expression_extract_outer']['expression_list'] -%}
                {% do run_query("drop view if exists "~autopie_prefix~"tmp_exp_type_"~run_object_id~" cascade") -%}
                {% do run_query("create or replace view "~autopie_prefix~"tmp_exp_type_"~run_object_id~" as select "~exp['expression']~" exp from "~autopie_prefix~"tmp_staging_view_expression_extract_1_"~run_object_id~" limit 1") -%}
                {% set data_type = run_query("select replace(udt_name, '_','') dt from information_schema.columns where table_schema||'.'||table_name = '"~autopie_prefix~"tmp_exp_type_"~run_object_id~"' and column_name = 'exp'") -%}
                {% do params.expressions.append(exp['alias']) -%}
                ({{exp['expression']}})::{{data_type.rows[0]['dt']}} as {{exp['alias']}}{{ ", " if not loop.last else "" }}
                {% do run_query("drop view if exists "~autopie_prefix~"tmp_exp_type_"~run_object_id~" cascade") -%}
                {% endfor -%}
        from expression_extract_1 t
    ),
    {% else -%}
    expression_extract_2 as
    (
        select * from expression_extract_1
    ),
    {% endif -%}
    {% if ingredients['filter_conjunctions'] | length > 0 -%}
    filter__3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
        where 1=1
        {%- for exp in ingredients['filter_conjunctions'] %}
        and {{exp}}
        {%- endfor %}
    ),
    {% else -%}
    filter__3 as
    (
        select /* filtering irrelevant records out */ *
        from expression_extract_2
    ),
    {% endif -%}
    {% if ingredients['incremental_update_on'] | length > 0 -%}
    incremental_extract__4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter__3
        where {{ingredients['incremental_update_on']}} >= to_date('{{autopie_run_start_date}}', 'YYYY-MM-DD')
          and {{ingredients['incremental_update_on']}}  < to_date('{{autopie_run_end_date}}', 'YYYY-MM-DD')
    ),
    {% else -%}
    incremental_extract__4 as
    (
        select /* retrieving incremental data - by date range only */ *
        from filter__3
    ),
    {% endif -%}
    {% if ingredients['deduplication'] | length > 0 -%}
    deduplication__5 as
    (
        select /* deduplicate by strategy */
               {{ingredients['deduplication']['grouping_key']}}
               {%- for exp in params.expressions %}
               {%- if exp not in ingredients['deduplication']['grouping_key'].split(',')|string %}
               {%- do params.aggregations.append(exp)%}
               {%- endif -%}
               {%- endfor %}
               {%- for exp in params.aggregations %}
               {%- if ingredients['deduplication']['preference']|lower == 'arbitrary' %}
               , max({{exp}}) as {{exp}}
               {%- else %}
               , (array_agg({{exp}} order by {{ingredients['deduplication']['preference']}}))[1] as {{exp}}
               {%- endif -%}
               {%- endfor %}
        from incremental_extract__4
        group by {{ingredients['deduplication']['grouping_key']}} 
    ),
    {% else -%}
    deduplication__5 as
    (
        select * from incremental_extract__4
    ),
    {% endif -%}
    {% if ingredients['coarsening'] | length > 0 -%}
    coarsening_6 as
    (
        select /* flatten all dimensions, fit into EAVT format */
              {{ingredients['coarsening']['entity_and_dims']}}
              {{", "~ingredients['coarsening']['date_expression'] if ingredients['coarsening']['date_expression']|length>0 else ""}}{{" as "~ingredients['coarsening']['date_alias'] if ingredients['coarsening']['date_alias']|length>0 else ""}}
              {{", "~ingredients['coarsening']['expire_expression'] if ingredients['coarsening']['expire_expression']|length>0 else ""}}{{" as "~ingredients['coarsening']['expire_alias'] if ingredients['coarsening']['expire_alias']|length>0 else ""}}
              {% if ingredients['coarsening']['aggregations'] == 'arbitrary' -%}
              {% for exp in params.expressions -%}
              {% if exp != ingredients['coarsening']['entity_and_dims'] and exp!=ingredients['coarsening']['date_expression'] and exp!=ingredients['coarsening']['expire_expression'] and exp|length > 1 and exp not in ingredients['coarsening']['entity_and_dims'].split(',')|string -%}
              , max({{exp}}) as {{exp}}
              {% endif -%}
              {% endfor -%}
              {% else -%}
              {% for exp in ingredients['coarsening']['aggregations'] -%}
              {% if exp['apply']|length>0 -%}
              {% if exp['apply']=='arbitrary' -%}
              {% for inner_exp in exp['column_list'] -%}
              , max({{inner_exp}}) as {{inner_exp}}
              {% endfor -%}
              {% elif exp['apply']=='unpivot' -%}
              {% for inner_exp in exp['val_list'] -%}
              , max(case when {{exp['cat_column']}}='{{inner_exp}}' then 1 else 0 end) as {{exp['alias_prefix']}}{{inner_exp.lower().replace(' ', '_').replace('-', '_').replace('.', '_').replace('/', '_').replace('+', '_').replace('!', '_').replace('@', '_').replace('#', '_').replace('$', '_').replace('%', '_').replace('^', '_').replace('&', '_').replace('*', '_').replace('(', '_').replace(')', '_').replace('<', '_').replace('>', '_').replace('?', '_').replace(':', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('[', '_').replace(']', '_').replace('|', '_').replace('\\', '_').replace('"', '_').replace("'", '_').replace('`', '_').replace('~', '_').replace('=', '_')}}
              {% endfor -%}
              {% else -%}
              -- not supported expression {{exp}}
              {% endif -%}
              {% else -%}
              , {{exp['expression']}} as {{exp['alias']}}
              {% endif -%}
              {% endfor -%}
              {% endif -%}
        from deduplication__5
        group by {{ingredients['coarsening']['entity_and_dims']}}{{", "~ingredients['coarsening']['date_expression'] if ingredients['coarsening']['date_expression']|length>0 else ""}}{{ ", "~ingredients['coarsening']['expire_expression'] if ingredients['coarsening']['expire_expression']|length>0 else "" }}
    )
    {% else -%}
    coarsening_6 as
    (
        select * from deduplication__5
    )
    {% endif -%}
    select  t.*,
            {{autopie_run_id}} autopie_run_id
    from coarsening_6 t
);

{{clean_intermediate}}

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
    alter  table {{autopie_prefix}}obj_fact_{{target_fact['id']}} add primary key ({{target_fact['entity_and_dims']}}, {{target_fact['date']}}{{", "~target_fact['expire'] if target_fact['expire']|length>0 else ""}});
    --
    create index ixrid_obj_fact_{{target_fact['id']}} on {{autopie_prefix}}obj_fact_{{target_fact['id']}}(row_id);
end if;
end
$$;

-- upsert staging additions into the permanent fact object
delete from {{autopie_prefix}}obj_fact_{{target_fact['id']}} where ({{target_fact['entity_and_dims']}}, {{target_fact['date']}}{{", "~target_fact['expire'] if target_fact['expire']|length>0 else ""}}) in (select {{target_fact['entity_and_dims']}}, {{target_fact['date']}}{{", "~target_fact['expire'] if target_fact['expire']|length>0 else ""}} from {{autopie_prefix}}tmp_staging_fact_{{run_object_id}});
insert into {{autopie_prefix}}obj_fact_{{target_fact['id']}} select * from {{autopie_prefix}}tmp_staging_fact_{{run_object_id}};
drop table {{autopie_prefix}}tmp_staging_fact_{{run_object_id}};

{%- endset %}

{% do run_query(clean_intermediate) -%}

{# /* return the SQL back to caller */ #}
{{sql}}

{% endmacro %}

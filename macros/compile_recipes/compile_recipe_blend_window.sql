{% macro compile_recipe_blend_window(autopie_prefix, ingredients, target_fact) %}

{% set run_object_id = range(10000, 99999) | random ~ range(10000, 99999) | random -%}
{% if env_var('DETERMINISTIC_RUN_ID', 'FALSE') == 'TRUE' %}{% set run_object_id = '0000000000' %}{% endif -%}

{% set autopie_run_id = env_var('AUTOPIE_RUN_ID', '0') -%}

{% set sql -%}
drop table if exists {{autopie_prefix}}tmp_alter_staging_{{run_object_id}};
create table {{autopie_prefix}}tmp_alter_staging_{{run_object_id}} as
(
  select  row_id,
          /* calculate window functions on whole fact table - TODO, will optimize this */
          {%- for exp in ingredients['window_expression_list'] %}
          {% if exp['ifnull'] | length > 0 -%}
          coalesce({{exp['expression']}}, {{exp['ifnull']}}) as {{exp['alias']}}{{ ", " if not loop.last else "" }}
          {%- else -%}
          {{exp['expression']}} as {{exp['alias']}}{{ ", " if not loop.last else "" }}
          {%- endif -%}
          {%- endfor %}
  from {{autopie_prefix}}obj_fact_{{target_fact}}
);

alter table {{autopie_prefix}}tmp_alter_staging_{{run_object_id}} add primary key (row_id);

-- check if not exists already, this should run once
{%- for exp in ingredients['window_expression_list'] %}
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

{%- endfor %}
update {{autopie_prefix}}obj_fact_{{target_fact}} as t
set 
    {%- for exp in ingredients['window_expression_list'] %}
    {{exp['alias']}} = m.{{exp['alias']}},
    {%- endfor %}
    autopie_run_id = {{autopie_run_id}}
from {{autopie_prefix}}tmp_alter_staging_{{run_object_id}} m 
where t.row_id = m.row_id;

drop table {{autopie_prefix}}tmp_alter_staging_{{run_object_id}};
{%- endset %}

{# /* return the SQL back to caller */ #}
{{sql}}

{% endmacro %}

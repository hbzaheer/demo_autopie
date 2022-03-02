{% macro run_directory() %}

{% set autopie_prefix =  env_var('AUTOPIE_PREFIX', 'autopie.') %}

{% set obtain_params %}
with read_directory as (select * from (select * from pg_ls_dir('./autopie_workdir') pg) iq where pg like '%.json'),
file_content as (select pg file_name, cast(pg_read_file(E'./autopie_workdir/'||pg, 0, 100000000) as json) json_content from read_directory)
select file_name, 
       json_content->>'recipe_type' recipe_type,
       json_content->>'ingredients' ingredients,
       json_content->>'target_fact' target_fact,
       json_content->>'source_fact' source_fact
  from file_content
order by file_name;
{% endset %}

{% set params_list = run_query(obtain_params) -%}

{% for param_row in params_list.rows %}
{{ log("\n--RUNNING FILE ---> "~param_row['file_name'], info=True) }}
{% set target_fact = "" if param_row['target_fact'] == None else param_row['target_fact'] %}
{% set target_fact = target_fact if fromjson(target_fact) == None else fromjson(target_fact) %}
{% set recipe_output = run_recipe(autopie_prefix, param_row['recipe_type'], fromjson(param_row['ingredients']), target_fact, param_row['source_fact']) -%}
{% endfor %}

{% endmacro %}

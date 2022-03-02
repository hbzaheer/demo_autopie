-- Use the `ref` function to select from other models

select *
from "postgres"."testsatmap"."my_first_dbt_model"
where id = 1
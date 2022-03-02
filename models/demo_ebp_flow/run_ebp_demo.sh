#!/bin/bash
cd models/demo_ebp_flow

# LOAD DEMO DATA
psql -U postgres -f demo_data_prep.sql -q

psql -U postgres -c "SET client_min_messages TO WARNING; drop schema if exists autopie_ebpdemo cascade;" -q
psql -U postgres -c "SET client_min_messages TO WARNING; create schema if not exists autopie_ebpdemo;" -q

# SET DETERMINISTIC IDS FOR TESTING TO PREVENT DIFFS
export DETERMINISTIC_RUN_ID=TRUE

# SET AUTOPIE SCHEMA AND PROJECT PREFIX
export AUTOPIE_PREFIX=autopie_ebpdemo.

# RUN runs only, INFO prints only, DEBUG both runs and prints
export AUTOPIE_RUN_MODE=DEBUG

# add a numeric incremental ID to keep up with most recent run. Use now() to assign epoch if possible
# export AUTOPIE_RUN_ID=$(date +%s)
export AUTOPIE_RUN_ID=1
export AUTOPIE_RUN_START_DATE=2020-01-01
export AUTOPIE_RUN_END_DATE=2021-01-01

#unlink $(psql -t -U postgres -c "show data_directory")/autopie_workdir 2> /dev/null
#ln -s $(pwd)/recipes $(psql -t -U postgres -c "show data_directory")/autopie_workdir

# process whole directory at once
#dbt run-operation run_directory | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/prepared_facts.sql

# Phase 1 - Prepare Facts
dbt run-operation run_recipe --args "$(cat recipes/phase1_collect_fact_calls.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase1_fact_calls.sql
dbt run-operation run_recipe --args "$(cat recipes/phase1_collect_fact_outcomes.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase1_fact_outcomes.sql
dbt run-operation run_recipe --args "$(cat recipes/phase1_collect_fact_crm.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase1_fact_crm.sql
dbt run-operation run_recipe --args "$(cat recipes/phase1_collect_fact_dispositions.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase1_fact_dispositions.sql


# Phase 2 - Joins
dbt run-operation run_recipe --args "$(cat recipes/phase2_blend_window_next_agent.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase2_window_next_agent.sql
dbt run-operation run_recipe --args "$(cat recipes/phase2_blend_join_disp_to_calls.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase2_join_disp_to_calls.sql
dbt run-operation run_recipe --args "$(cat recipes/phase2_blend_join_outc_to_calls.json)" | grep -v "Running with dbt=" | grep -vE "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]" > ./prepared_scripts/phase2_join_outc_to_calls.sql

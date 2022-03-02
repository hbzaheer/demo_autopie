import streamlit as st
import pandas as pd
from utils_database import read_query
from streamlit_ace import st_ace
import utils_commons as uc


def render_schema_details():
    
    # st.set_page_config(layout="wide")
    container_schema_details = st.container()
    container_schema_details.subheader("Schema Details")
    col_schemas, col_tables = container_schema_details.columns([1, 1])

    if 'connection' in st.session_state:

        excluded_schemas = "('public', 'pg_catalog', 'pg_toast', 'pg_aoseg', 'pg_bitmapindex', 'information_schema', 'gp_toolkit')"
        query_schemas = f"select schema_name from information_schema.schemata where schema_name not in {excluded_schemas}"
        #print(query_schemas)
        
        list_schemas = read_query(query_schemas)
        selected_schema = col_schemas.selectbox("Schema", list_schemas)

        if selected_schema:
            st.session_state.selected_schema = selected_schema

            query_tables = f"select table_name from information_schema.tables where table_schema = '{st.session_state.selected_schema}'"
            #print(query_tables)
            
            list_tables = read_query(query_tables)
            selected_table = col_tables.selectbox("Table", list_tables)

            if selected_table:
                st.session_state.selected_table = selected_table
                
                query_selected_table = f"select * from {st.session_state.selected_schema}.{st.session_state.selected_table} limit 100"
                #print(query_selected_table)
                
                st.subheader("Raw Data")
                st.write(read_query(query_selected_table))

                query_columns = f"select column_name from information_schema.columns where table_name = '{st.session_state.selected_table}'"
                #print(query_columns)


                container_extract = st.container()

                with container_extract:

                    st.subheader("Define Expressions")

                    col_name, col_exp, col_alias, col_del = st.columns([3, 5, 3, 1])
                    col_name.markdown("Column")
                    col_exp.markdown("Expression")
                    col_alias.markdown("Alias")

                    container_columns = st.container()

                    container_buttons = st.container()
                    col_button_add, col_button_sql = container_buttons.columns([1, 10])
                    button_add_column = col_button_add.button("Add")
                    button_prepare_sql = col_button_sql.button("Prepare SQL")

                    st.session_state.dict_columns = {}

                    if 'column_count' not in st.session_state:
                        st.session_state.column_count = 1

                    if button_add_column:
                        st.session_state.column_count += 1

                    if 'delete_column' not in st.session_state:
                        st.session_state.delete_column = -1

                    for i in range(st.session_state.column_count):

                        if st.session_state.delete_column == i:
                            print(f"Inside If Condition {i}: ", st.session_state.delete_column)
                            st.session_state.delete_column = -1
                            st.session_state.column_count -= 1
                            continue

                        col_name, col_exp, col_alias, col_del = container_columns.columns([3, 5, 3, 1])

                        name_column = col_name.selectbox('', read_query(query_columns), 0, key='column_'+str(i))
                        expression = col_exp.text_input("", value=name_column, key='expression_'+str(i))
                        alias = col_alias.text_input("", value=name_column, key='alias_'+str(i))
                        with col_del:
                            st.markdown("##")
                            button_delete = st.button("x", key="delete_"+str(i))
                            if button_delete:
                                st.session_state.delete_column = i
                                print(f"On Click {i}: ", st.session_state.delete_column)
                                print(f"Column '{name_column}' deleted!")
                                st.experimental_rerun()

                        st.session_state.dict_columns[f'Column_{i}'] = {'name_column': name_column, 'expression': expression, 'alias': alias}

                #print(json.dumps(st.session_state.dict_columns, sort_keys=True, indent=4))
                container_columns.markdown(f"*** COLUMN COUNT : {st.session_state.column_count}")
                
                column_extracts = uc.prepare_extract_query(st.session_state.dict_columns)

                if button_prepare_sql:
                    st.subheader("Extraction Script")    
                    query_extract = st_ace(language='sql', theme='dracula', value=f"select\n{column_extracts}\nfrom {selected_schema}.{selected_table}\n;")
                    
                    if query_extract:
                        try:
                            result_set = read_query(query_extract)
                            st.subheader('Extracted Data')
                            st.write(result_set)
                        except Exception as e:
                            st.error("Exception " + e.__class__ + "occurred!")





            
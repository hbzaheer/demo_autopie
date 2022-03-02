import streamlit as st
import utils_database as db
import pandas as pd
from view_schema_details import render_schema_details
import utils_commons as uc


def render_db_connection():
    st.title("Connect to the Database")

    with st.form("form_db_connection"):
        input_column, blank = st.columns([2, 2])

        text_db_host = input_column.text_input("Host IP", value="127.0.0.1")
        text_db_name = input_column.text_input("Database", value="postgres")
        text_db_user = input_column.text_input("Username", value="postgres")
        text_db_pass = input_column.text_input("Password", type="password")
        text_db_port = input_column.text_input("Port", value=5432)

        if st.form_submit_button("Connect"):
            connection = db.db_connect(
                text_db_host,
                text_db_name,
                text_db_port,
                text_db_user,
                text_db_pass
            )

            if connection:
                st.session_state.connection = connection
                uc.set_current_page('schema_details')
                st.experimental_rerun()
            else:
                st.error("Error while connecting to PostgreSQL!")

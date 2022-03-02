import psycopg2
import streamlit as st
import pandas as pd


@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def db_connect(host, database, port, user, password):
    try:
        connection = psycopg2.connect(
                    host = host,
                    database = database,
                    port = port,
                    user = user,
                    password = password
                )

        # Print PostgreSQL Connection properties
        # print(connection.get_dsn_parameters(), "\n")

        # Print PostgreSQL version
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to", record, "\n")
        cursor.close()
        
        return connection
    
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL: {}".format(error))
        return False


def close_connection(connection):
    if connection:        
        connection.close()
        print("PostgreSQL connection is closed!")
        return True


def read_query(query):
    try:
        return pd.read_sql_query(query, con=st.session_state.connection)
    except Exception as e:
        print("Oops!", e.__class__, "occurred.")
        return pd.DataFrame({'column': []})
        


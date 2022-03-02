import streamlit as st


def get_current_page():
    if 'current_page' not in st.session_state:
        set_current_page('db_connection')
    
    return st.session_state.current_page


def set_current_page(page_id: str):
    st.session_state.current_page = page_id


def prepare_extract_query(object: dict):
    list_columns = []
    for value in object.values():
        list_columns.append(f"\t{value['name_column']} as {value['alias']}")
    return ",\n".join(list_columns)


def clear_session_var(x: str):
    if st.session_state[x]:
        del st.session_state[x]
        print("Session Variable '{}' deleted!". format(x))

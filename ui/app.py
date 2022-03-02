import utils_commons as uc
from view_db_connection import render_db_connection
from view_schema_details import render_schema_details


def app():
    try:
        eval("render_" + uc.get_current_page() + "()")
    
    except NameError:
        print("Render function cannot be found!")


if __name__ == '__main__':
    app()
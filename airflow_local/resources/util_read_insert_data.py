import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import papermill as pm


def process_notebook_and_generate_csv(notebook_path, output_notebook_path, output_csv_path=None, parameters=None):
    """
    Process a Jupyter notebook and generate a cleaned CSV output.
    
    Args:
        - notebook_path: The path to the input Jupyter notebook.
        - output_notebook_path: The path to save the output notebook after execution.
        - output_csv_path: The path to save the cleaned data as a CSV file.
        - parameters: A dictionary of parameters to pass to the notebook.
    """
    pm.execute_notebook(
        notebook_path, 
        output_notebook_path,  
        parameters=parameters
    )
    
    if output_csv_path:
        cleaned_data = pd.read_csv(output_csv_path)
        return cleaned_data

def load_excel_to_db(file_path, postgres_conn_id, table_name, sheet_name, notebook_path, output_notebook_path, output_csv_path):
    """
    Load data from a specific sheet in an Excel file into a PostgreSQL table, after cleaning up data using a Jupyter notebook.
    
    Args:
        - file_path (str): The path to the Excel file.
        - postgres_conn_id (str): The connection ID for the PostgreSQL database in Airflow.
        - table_name (str): The target PostgreSQL table name.
        - sheet_name (str): The name of the sheet to load.
        - notebook_path (str): The path to the Jupyter notebook that performs cleanup operations.
        - output_notebook_path (str): The path to save the output notebook after execution.
        - output_csv_path (str): The path to save the cleaned data as a CSV file.

    """

    cleaned_data = process_notebook_and_generate_csv(
        notebook_path=notebook_path,
        output_notebook_path=output_notebook_path,
        output_csv_path=output_csv_path,
        parameters={'file_path': file_path, 'sheet_name': sheet_name}
    )
    
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()

    engine = create_engine(f'postgresql://{conn.info.user}:{conn.info.password}@{conn.info.host}:{conn.info.port}/{conn.info.dbname}')

    cleaned_data.to_sql(table_name, engine, if_exists='append', index=False)
    
    print(f"Data from sheet '{sheet_name}' loaded into table '{table_name}' after cleanup.")

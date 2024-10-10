# database_utils.py

import sqlite3
import pandas as pd
from logging_utils import log_progress

def load_to_db(df, db_name, table_name, log_file):
    ''' Load the dataframe into an SQLite database. '''
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        log_progress(f"Data successfully loaded to {table_name} table", log_file)
        return conn
    except Exception as e:
        log_progress(f"Database loading failed: {e}", log_file, error=True)
        return None

def run_query(query_statement, conn, log_file):
    ''' Run a SQL query on the database connection. '''
    try:
        query_output = pd.read_sql(query_statement, conn)
        print(query_output)
    except Exception as e:
        log_progress(f"Query execution failed: {e}", log_file, error=True)

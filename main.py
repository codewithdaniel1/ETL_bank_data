# main.py

from etl_utils import extract, transform, load_to_csv
from database_utils import load_to_db, run_query
from logging_utils import log_progress

# Declaring known values
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "./exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
output_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "./code_log.txt"

log_progress("Preliminaries complete. Initiating ETL process", log_file)

# Call extract() function
df = extract(url, table_attribs, log_file)
if df is not None:
    log_progress("Data extraction complete. Initiating transformation process", log_file)

    # Call transform() function
    df = transform(df, csv_path, log_file)
    if df is not None:
        log_progress("Data transformation complete. Initiating loading process", log_file)

        # Save to CSV
        load_to_csv(df, output_path, log_file)

        # Load to Database
        conn = load_to_db(df, db_name, table_name, log_file)

        if conn:
            log_progress("Data loaded to Database as a table, executing queries", log_file)

            # Example queries
            run_query(f"SELECT * from {table_name}", conn, log_file)
            run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", conn, log_file)
            run_query(f"SELECT MAX(MC_GBP_Billion) FROM {table_name}", conn, log_file)
            run_query(f"SELECT Min(MC_GBP_Billion) FROM {table_name}", conn, log_file)

            conn.close()
            log_progress("Server connection closed", log_file)
    else:
        log_progress("Transformation failed. Aborting process.", log_file, error=True)
else:
    log_progress("Extraction failed. Aborting process.", log_file, error=True)

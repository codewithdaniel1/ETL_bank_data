from etl_utils import transform, load_to_csv, extract_largest, extract_united_states, extract_southeast_asia, extract_europe, extract_latin
from database_utils import load_to_db, run_query
from logging_utils import log_progress
import pandas as pd

# Declaring known values
url_largest = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
url_united_states = "https://web.archive.org/web/20230314195644/https://en.wikipedia.org/wiki/List_of_largest_banks_in_the_United_States"
url_southeast = "https://web.archive.org/web/20230318102354/https://en.wikipedia.org/wiki/List_of_largest_banks_in_Southeast_Asia"
url_europe = "https://web.archive.org/web/20231217052339/https://en.wikipedia.org/wiki/List_of_banks_in_Europe"
url_latin = "https://web.archive.org/web/20231222103658/https://en.wikipedia.org/wiki/List_of_largest_banks_in_Latin_America"
csv_path = "./exchange_rate.csv"
table_attribs = ["Name", "TA_USD_Billion"]
output_path = "./Combined_banks_data.csv"
db_name = "./Banks.db"
table_name = "Combined_banks"
log_file = "./code_log.txt"

log_progress("Preliminaries complete. Initiating ETL process", log_file)

# Extract data from the "United States Banks"
df_us = extract_united_states(url_united_states, table_attribs, log_file)
if df_us is not None:
    log_progress("United States Banks data extraction complete.", log_file)
else:
    log_progress("United States Banks data extraction failed. Aborting process.", log_file, error=True)
    df_us = pd.DataFrame()  # Initialize an empty DataFrame in case extraction fails

# Extract data from the "Southeast Asia Banks"
df_southeast = extract_southeast_asia(url_southeast, table_attribs, log_file)
if df_southeast is not None:
    log_progress("Largest Banks data extraction complete.", log_file)
else:
    log_progress("Largest Banks data extraction failed. Aborting process.", log_file, error=True)
    df_southeast = pd.DataFrame()  # Initialize an empty DataFrame in case extraction fails

# Extract data from the "European Banks"
df_europe = extract_europe(url_europe, table_attribs, log_file)
if df_europe is not None:
    log_progress("Largest European Banks data extraction complete.", log_file)
else:
    log_progress("Largest European Banks data extraction failed. Aborting process.", log_file, error=True)
    df_europe = pd.DataFrame()  # Initialize an empty DataFrame in case extraction fails

# Extract data from the "Latin America Banks"
df_latin = extract_latin(url_latin, table_attribs, log_file)
if df_latin is not None:
    log_progress("Largest Latin America Banks data extraction complete.", log_file)
else:
    log_progress("Largest Latin America Banks data extraction failed. Aborting process.", log_file, error=True)
    df_latin = pd.DataFrame()  # Initialize an empty DataFrame in case extraction fails

# Extract data from the "Largest Banks"
df_largest = extract_largest(url_largest, table_attribs, log_file)
if df_largest is not None:
    log_progress("Largest Banks data extraction complete.", log_file)
else:
    log_progress("Largest Banks data extraction failed. Aborting process.", log_file, error=True)
    df_largest = pd.DataFrame()  # Initialize an empty DataFrame in case extraction fails

# Combine the data from both sources
combined_df = pd.concat([df_us, df_southeast, df_largest, df_europe, df_latin], ignore_index=True)
if combined_df.empty:
    log_progress("Combined dataset is empty. Aborting process.", log_file, error=True)
else:
    log_progress("Data successfully combined.", log_file)

    # Step 4: Transform the combined data
    combined_df = transform(combined_df, csv_path, log_file)
    if combined_df is not None:
        log_progress("Data transformation complete.", log_file)

        # Step 5: Save the combined data to a CSV
        load_to_csv(combined_df, output_path, log_file)

        # Step 6: Load the combined data to the database
        conn = load_to_db(combined_df, db_name, table_name, log_file)
        if conn:
            log_progress("Data loaded to Database as a table. Executing queries.", log_file)

            # Example queries
            run_query(f"SELECT * from {table_name} ORDER BY Location DESC", conn, log_file)
            run_query(f"SELECT AVG(TA_GBP_Billion) FROM {table_name}", conn, log_file)
            run_query(f"SELECT * FROM {table_name} WHERE Location = 'United States'", conn, log_file)


            conn.close()
            log_progress("Server connection closed", log_file)
        else:
            log_progress("Failed to load data to Database. Aborting process.", log_file, error=True)
    else:
        log_progress("Data transformation failed. Aborting process.", log_file, error=True)

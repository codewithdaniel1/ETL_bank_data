import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time
from logging_utils import log_progress
import re

# Helper function to extract numeric values from strings
def extract_numeric_value(text):
    ''' Extract the numeric part from a string using regex. '''
    match = re.search(r'[\d,]+(?:\.\d+)?', text)
    if match:
        return float(match.group(0).replace(",", ""))
    return None  # Return None if no numeric part is found


# Extraction function from 'etl_united_states.py'
def extract_united_states(url, table_attribs, log_file, retries=3, delay=5):
    ''' Extract data from a webpage (United States) with retries. '''
    attempt = 0
    while attempt < retries:
        try:
            page = requests.get(url).text
            soup = BeautifulSoup(page, "html.parser")

            df = pd.DataFrame(columns=table_attribs)
            tables = soup.find_all("tbody")
            rows = tables[1].find_all("tr")

            for row in rows:
                col = row.find_all("td")
                if len(col) != 0:
                    name = col[1].find("a").text
                    
                    if col[3].contents[0].name == "a":
                        value_text = col[3].contents[1].strip()
                    else:
                        value_text = col[3].contents[0].strip()

                    # Convert to float using the helper function
                    ta_usd_billion = extract_numeric_value(value_text)
                    data_dict = {"Name": name, "Country": "United States", "TA_USD_Billion": ta_usd_billion}
                    
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)

            log_progress("Data extraction successful", log_file)
            return df
        except Exception as e:
            log_progress(f"Attempt {attempt+1}: Data extraction failed: {e}", log_file, error=True)
            attempt += 1
            time.sleep(delay)
    log_progress("All attempts failed. Data extraction aborted.", log_file, error=True)
    return None


# Extraction function from 'etl_utils_largest.py'
def extract_largest(url, table_attribs, log_file, retries=3, delay=5):
    ''' Extract data from a webpage (Largest) with retries. '''
    attempt = 0
    while attempt < retries:
        try:
            page = requests.get(url).text
            soup = BeautifulSoup(page, "html.parser")

            df = pd.DataFrame(columns=table_attribs)
            tables = soup.find_all("tbody")
            rows = tables[1].find_all("tr")

            for row in rows:
                col = row.find_all("td")
                if len(col) != 0:
                    data_dict = {"Name": col[1].find_all("a")[1]["title"],
                                 "Country": "n/a",
                                 "TA_USD_Billion": float(col[2].contents[0][:-1].replace(",", ""))}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)

            log_progress("Data extraction successful", log_file)
            return df
        except Exception as e:
            log_progress(f"Attempt {attempt+1}: Data extraction failed: {e}", log_file, error=True)
            attempt += 1
            time.sleep(delay)
    log_progress("All attempts failed. Data extraction aborted.", log_file, error=True)
    return None


# Common transformation function
def transform(df, csv_path, log_file):
    ''' Transform the data by adding currency conversion columns. '''
    try:
        exchange_rate = pd.read_csv(csv_path).set_index("Currency").to_dict()["Rate"]
        df["TA_GBP_Billion"] = np.round(df["TA_USD_Billion"] * exchange_rate["GBP"], 2)
        df["TA_EUR_Billion"] = np.round(df["TA_USD_Billion"] * exchange_rate["EUR"], 2)
        df["TA_INR_Billion"] = np.round(df["TA_USD_Billion"] * exchange_rate["INR"], 2)
        log_progress("Data transformation successful", log_file)
        return df
    except Exception as e:
        log_progress(f"Data transformation failed: {e}", log_file, error=True)
        return None


# Common function to save the dataframe to CSV
def load_to_csv(df, output_path, log_file):
    ''' Save the dataframe to a CSV file. '''
    try:
        df.to_csv(output_path, index=False)
        log_progress(f"Data successfully saved to {output_path}", log_file)
    except Exception as e:
        log_progress(f"CSV saving failed: {e}", log_file, error=True)

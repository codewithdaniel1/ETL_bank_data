# etl_utils.py

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time
from logging_utils import log_progress

def extract(url, table_attribs, log_file, retries=3, delay=5):
    ''' Extract data from a webpage with retries. '''
    attempt = 0
    while attempt < retries:
        try:
            page = requests.get(url).text
            soup = BeautifulSoup(page, "html.parser")

            df = pd.DataFrame(columns=table_attribs)
            tables = soup.find_all("tbody")
            rows = tables[0].find_all("tr")

            for row in rows:
                col = row.find_all("td")
                if len(col) != 0:
                    data_dict = {"Name": col[1].find_all("a")[1]["title"],
                                 "MC_USD_Billion": float(col[2].contents[0][:-1])}
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

def transform(df, csv_path, log_file):
    ''' Transform the data by adding currency conversion columns. '''
    try:
        exchange_rate = pd.read_csv(csv_path).set_index("Currency").to_dict()["Rate"]
        df["MC_GBP_Billion"] = np.round(df["MC_USD_Billion"] * exchange_rate["GBP"], 2)
        df["MC_EUR_Billion"] = np.round(df["MC_USD_Billion"] * exchange_rate["EUR"], 2)
        df["MC_INR_Billion"] = np.round(df["MC_USD_Billion"] * exchange_rate["INR"], 2)
        log_progress("Data transformation successful", log_file)
        return df
    except Exception as e:
        log_progress(f"Data transformation failed: {e}", log_file, error=True)
        return None

def load_to_csv(df, output_path, log_file):
    ''' Save the dataframe to a CSV file. '''
    try:
        df.to_csv(output_path, index=False)
        log_progress(f"Data successfully saved to {output_path}", log_file)
    except Exception as e:
        log_progress(f"CSV saving failed: {e}", log_file, error=True)

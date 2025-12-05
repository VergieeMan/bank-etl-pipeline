# -*- coding: utf-8 -*-
"""
Created on Thu Dec  4 10:58:14 2025

@author: Vergi
"""

import pandas as pd 
from bs4 import BeautifulSoup
import requests
import sqlite3
import os
import logging

#initiate
target_file = "Largest_banks_data.csv"
log_file = "code_log.txt"
folder = "C:/Users/Vergi/.spyder-py3/ETL - Final/"

#Setup logging
logging.basicConfig(
    filename=log_file,
    level = logging.INFO,
    format= '%(asctime)s | %(message)s',
    datefmt = '%Y-%h-%d %H:%M:%S'
    )

#log_progress
def log_progress(message):
    logging.info(message)

#Extract
url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')
dataframe = []
def extract():
    for row in rows:
        col = row.find_all('td')
        if len(col) > 0:
            try:
                Name = col[1].get_text(strip=True)
                MC_USD_Billion_raw = col[2].get_text(strip=True)
                MC_USD_Billion_clean = MC_USD_Billion_raw.replace(',', '').replace('-','').replace(' ','').replace('\xa0', '')
                if not MC_USD_Billion_clean:
                    continue
    
                MC_USD_Billion = float(MC_USD_Billion_clean)
                dataframe.append({
                    "Name": Name,
                    "MC_USD_Billion": MC_USD_Billion
                })
            except Exception as e:
                print(f'ERROR extract data - {str(e)}')
                df = pd.DataFrame(columns = ["Name", "MC_USD_Billion"])
                return df
                
    extracted_data = pd.DataFrame(dataframe) if dataframe else pd.DataFrame(columns=["Name", "MC_USD_Billion"])
    return extracted_data
    print(extracted_data)

log_progress("Extract task started")
extract()
log_progress("Extract task Completed")

#Transform
ex = pd.read_csv(os.path.join(folder,"exchange_rate.csv"))
def transform(data):
    if data.empty:
        print("WARNING - No data to be transformed")
        return data
    data['MC_GBP_Billion'] = round(data['MC_USD_Billion']*ex.loc[ex['Currency']=='GBP','Rate'].values[0],2)
    data['MC_EUR_Billion'] = round(data['MC_USD_Billion']*ex.loc[ex['Currency']=='EUR','Rate'].values[0],2)
    data['MC_INR_Billion'] = round(data['MC_USD_Billion']*ex.loc[ex['Currency']=='INR','Rate'].values[0],2)
    return data

log_progress("Transform task started")
extracted_data = extract()
transformed_data = transform(extracted_data)
print(transformed_data)
log_progress("Transform task Completed")

#Load data
#Load to csv
def load_to_csv(target_file, transformed_data):
    try:
        transformed_data.to_csv(target_file)
    except Exception as e:
        print(f"ERRROR - Loading to csv Failed: {str(e)}")
log_progress("Loading task started")
load_to_csv(target_file,transformed_data)
log_progress("Loading task Completed")

#Load to SQL
db_name = "Banks.db"
table_name = "Largest_banks"
conn = sqlite3.connect(db_name)
def load_to_sql(transformed_data):
    try:
        transformed_data.to_sql(table_name, conn, if_exists = 'replace', index = False)
    except Exception as e:
        print(f"ERRPR - Loading to sql Failed: {str(e)} ")
log_progress("Loading task started")
load_to_sql(transformed_data)
log_progress("Loading task Completed")

#run query
query = f"SELECT AVG(MC_GBP_Billion) FROM {table_name} LIMIT 5"
def run_query(query, conn):
    df = pd.read_sql(query, conn)
    pd.set_option('display.max_columns', None)
    print(df) 

run_query(query, conn)
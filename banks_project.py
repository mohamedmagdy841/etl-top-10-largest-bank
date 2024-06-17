import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'exchange_rate.csv'
table_attrs_extract = ['Name', 'MC_USD_Billion']
table_attrs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_csv_path = 'Largest_banks_data.csv'
database_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

def log_progress(msg):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp + " : " + msg + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    data = data.find_all('tbody')
    rows = data[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            bank_name = col[1].find_all('a')[1]
            df1 = pd.DataFrame({'Name': bank_name.contents[0], 'MC_USD_Billion': float(col[2].contents[0].strip())}, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate_df = pd.read_csv(csv_path)
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rate_df.loc[1,'Rate'],2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate_df.loc[0,'Rate'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate_df.loc[2,'Rate'], 2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

if __name__ == '__main__':
    log_progress("Preliminaries complete. Initiating ETL process")
    extracted_df = extract(url, table_attrs_extract)
    log_progress("Data extraction complete. Initiating Transformation process")

    transformed_df = transform(extracted_df, csv_path)
    log_progress("Data transformation complete. Initiating loading process")

    load_to_csv(transformed_df, output_csv_path)
    log_progress("Data saved to CSV file")

    conn = sqlite3.connect(database_name)
    log_progress('SQL Connection initiated')

    load_to_db(transformed_df, conn,table_name)
    log_progress('Data loaded to Database as table. Running the query')

    run_query(f"SELECT * FROM Largest_banks", conn)
    run_query(f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
    run_query(f"SELECT Name from Largest_banks LIMIT 5", conn)
    log_progress('Process Complete')

    conn.close()
    log_progress('Server Connection closed')

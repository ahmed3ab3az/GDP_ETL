# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import numpy as np 
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime 


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''





def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns= table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if col and col[0].find('a') and  '—' not in col[2]:
            data_dic = {
                'Country': col[0].a.contents[0],
                'GDP_USD_millions':col[2].contents[0]
            }
            df1 = pd.DataFrame(data_dic , index=[0])
            df = pd.concat([df,df1] , ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',','').astype(float)
    df['GDP_USD_millions'] = round(df['GDP_USD_millions'] / 1000 , 2)
    df.rename(columns = {'GDP_USD_millions' :'GDP_USD_billions' },inplace = True)
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists = 'replace', index =False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_res = pd.read_sql(query_statement,sql_connection)
    print(query_statement)
    print(query_res)





def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    time_form = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = datetime.strftime(now,time_form)
    with open('./GDP_Data/etl_project_log.txt' , 'a') as log:
        log.write(timestamp + ' : ' + message + '\n')







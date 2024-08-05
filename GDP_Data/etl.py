# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime 
import matplotlib.pyplot as plt
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
    return query_res

def validate_data(df):
    if df.isnull().values.any():
        log_progress("Data validation failed: Missing values detected.")
        raise ValueError("Data contains missing values")
    if not all(df['GDP_USD_billions'] >= 0):
        log_progress("Data validation failed: Negative GDP values detected.")
        raise ValueError("Data contains negative GDP values")
    log_progress("Data validation passed.")
    return True

def visualize_data(df, output_path, top_n=10):
    """
    Create a bar chart of the top N countries by GDP.
    
    Args:
    df (pandas.DataFrame): The dataframe containing the GDP data.
    output_path (str): The path where the visualization will be saved.
    top_n (int): The number of top countries to display (default is 10).
    
    Returns:
    None
    """
    # Sort the dataframe by GDP (descending) and select top N countries
    top_countries = df.sort_values('GDP_USD_billions', ascending=False).head(top_n)
    
    # Create a bar plot
    plt.figure(figsize=(12, 6))
    bars = plt.bar(top_countries['Country'], top_countries['GDP_USD_billions'])
    
    # Customize the plot
    plt.title(f'Top {top_n} Countries by GDP (Billions USD)')
    plt.xlabel('Country')
    plt.ylabel('GDP (Billions USD)')
    plt.xticks(rotation=45, ha='right')
    
    # Add value labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'{height:,.2f}',
                 ha='center', va='bottom')
    
    # Adjust layout and save the plot
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    
    log_progress(f'Data visualization saved to {output_path}')


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    time_form = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = datetime.strftime(now,time_form)
    with open('./GDP_Data/etl_project_log.txt' , 'a') as log:
        log.write(timestamp + ' : ' + message + '\n')

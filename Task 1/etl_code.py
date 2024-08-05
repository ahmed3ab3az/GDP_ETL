import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 
def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process) 
    return dataframe 
def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=['car_model' , "year_of_manufacture", "price","fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for row in root:
        car_model = row.find('car_model').text
        year_of_manufacture = row.find('year_of_manufacture').text
        price = float(row.find('price').text)
        fuel = row.find("fuel").text
        dataframe = pd.concat([dataframe, pd.DataFrame({'car_model' : [car_model] , 'year_of_manufacture' : [year_of_manufacture] , 'price' : [price] , "fuel" : [fuel] })], ignore_index=True)
    return dataframe 
def extract():
    extraced_data = dataframe = pd.DataFrame(columns=['car_model' , "year_of_manufacture", "price","fuel"])

    # process all csv files
    for file in glob.glob('datasource/*.csv'):
        extraced_data = pd.concat([extraced_data , extract_from_csv(file)] , ignore_index=True)


    # process all json files
    for file in glob.glob('datasource/*.json'):
        extraced_data = pd.concat([extraced_data , extract_from_json(file)] , ignore_index=True)

    # process all xml files
    for file in glob.glob('datasource/*.xml'):
        extraced_data = pd.concat([extraced_data , extract_from_xml(file)] , ignore_index=True)
    return extraced_data
def transform(data):
    ''' round off 'price' to two decimals  '''
    data['price'] = round(data['price'] , 2)
    return data
def load_data(target_file , transformed_data):
    transformed_data.to_csv(target_file)
def log_progress(message):
    timestamp_form = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_form)
    with open(log_file,'a') as log:
        log.write(timestamp + ' , ' + message + '\n')


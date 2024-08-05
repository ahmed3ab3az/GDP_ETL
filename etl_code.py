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
    dataframe = pd.DataFrame(columns=['name' , "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for row in root:
        name = row.find('name').text
        height = float(row.find('height').text)
        weight = float(row.find('weight').text)
        dataframe = pd.concat([dataframe, pd.DataFrame({'name' : [name] , 'height' : [height] , 'weight' : [weight] })], ignore_index=True)
    return dataframe 
def extract():
    extraced_data = dataframe = pd.DataFrame(columns=['name' , "height", "weight"])

    # process all csv files
    for file in glob.glob('source/*.csv'):
        extraced_data = pd.concat([extraced_data , extract_from_csv(file)] , ignore_index=True)


    # process all json files
    for file in glob.glob('source/*.json'):
        extraced_data = pd.concat([extraced_data , extract_from_json(file)] , ignore_index=True)

    # process all xml files
    for file in glob.glob('source/*.xml'):
        extraced_data = pd.concat([extraced_data , extract_from_xml(file)] , ignore_index=True)
    return extraced_data
def transform(data):
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = round(data['height'] * 0.0254 , 2)
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = round(data.weight * 0.45359237,2) 
    return data
def load_data(target_file , transformed_data):
    transformed_data.to_csv(target_file)
def log_progress(message):
    timestamp_form = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_form)
    with open(log_file,'a') as log:
        log.write(timestamp + ' , ' + message + '\n')


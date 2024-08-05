import sqlite3
import pandas as pd

conn = sqlite3.Connection("STAFF.db")

Table_name = 'Department'
attributes = ['DEPT_ID' , 'DEP_NAME' , 'MANAGER_ID' , 'LOC_ID']
file_path = 'Task 3/Departments.csv'
df = pd.read_csv(file_path , names = attributes)

df.to_sql(Table_name , conn, if_exists = 'replace' , index =False)
print('Table is Ready!!!!')

query = f'Select * from {Table_name}'
query_res = pd.read_sql(query,conn)
print(query)
print(query_res)

query = f'Select DEP_NAME from {Table_name}'
query_res = pd.read_sql(query,conn)
print(query)
print(query_res)

query = f'Select count(*) from {Table_name}'
query_res = pd.read_sql(query,conn)
print(query)
print(query_res)
import numpy as np
import pandas as pd 
import pymssql
import sqlalchemy
from pymongo import MongoClient
from prefect import task, flow
from datetime import datetime, timedelta
from urllib.parse import quote


def create_sql_connection(flag=0):
    server = '***.**.*.**'; database = '****'; user_id = '****'; password = "*******"; password = quote(password)
    sql_server_connection_string = f"mssql+pymssql://{user_id}:{password}@{server}:1433/{database}"
    sql_engine = sqlalchemy.create_engine(sql_server_connection_string)
    if flag == 1:
        connection = sql_engine.raw_connection()
    else: 
        connection = sql_engine.connect()   
    return connection


def create_mongodb_connection():
    mongo_uri = "******************************************************"
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client.roava
    return mongo_client, mongo_db


def handle_duplicate_column_names(df):
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item.lower()
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item.lower(), counter)
        new_columns.append(newitem)
    df.columns = new_columns
    return df
    

def flatten_dict(data, parent_key="", sep="_"):
    items = []
    if data is None:
        return {}
    if not isinstance(data, dict):
        return {parent_key: data} if parent_key else {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)


def process_collection(collection_name, mongo_db):
    collection = mongo_db[collection_name]
    collection_documents = list(collection.find())
    docs_df = pd.DataFrame(collection_documents)
    nested_data = {}
    flat_data = docs_df.copy()
    flat_data['load_date'] = pd.Timestamp.now()
    flat_data.rename(columns={"id": "idx"}, inplace=True)
    for col in ['status_ts', 'statusTs', 'ts']:
        if col in flat_data.columns:
            flat_data[col] = pd.to_numeric(flat_data[col], errors='coerce')
            flat_data[col] = flat_data[col].fillna(0).astype(int)
    if 'status_ts' in flat_data.columns and 'statusTs' in flat_data.columns: 
        flat_data.loc[flat_data['status_ts'] == 0, 'status_ts'] = flat_data['statusTs']       
    for col in flat_data.columns:
        if flat_data[col].apply(lambda x: isinstance(x, (dict, list))).any():
            #print(flat_data[col].head())
            if flat_data[col].apply(lambda x: isinstance(x, dict)).any():
                #print('nested document is a dictionary')
                nested_data[col] = pd.DataFrame(flat_data[col].apply(lambda x: flatten_dict(x) if isinstance(x, dict) else {}).tolist(), index=flat_data.index)
                nested_data[col].reset_index(inplace=True)
                nested_data[col].rename(columns={"index": "ID"}, inplace=True)
                flat_data[col + '_fk'] = flat_data.index
                flat_data = flat_data.drop(columns=[col])
            elif flat_data[col].apply(lambda x: isinstance(x, list)).any():
                #print('nested document is a list')
                flat_data_copy = flat_data.copy()
                flat_data_copy = flat_data_copy.explode(col)
                nested_data[col] = pd.DataFrame(flat_data_copy[col].apply(lambda x: flatten_dict(x) if isinstance(x, dict) else {}).tolist(), 
                                                index=flat_data_copy.index)
                nested_data[col].reset_index(inplace=True)
                nested_data[col].rename(columns={"index": "ID"}, inplace=True)
                flat_data[col + '_fk'] = flat_data.index
                flat_data = flat_data.drop(columns=[col])
                flat_data_copy = ''
    return collection_name, nested_data, flat_data


def convert_to_sql_compatible_datatypes(df):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input is not a pandas DataFrame")
    for col in df.columns:
        try:
            if df[col].dtype == 'object' or 'datetime' in str(df[col].dtype):
                df[col] = df[col].astype(str)
        except AttributeError as e:
            print(f"Error converting column '{col}': {e}")
    return df


def execute_sql_procedure(raw_connection, procedure_name):
    try:
        cursor = raw_connection.cursor()
        cursor.callproc(procedure_name)
        cursor.close()
        raw_connection.commit()
        print('Query executed')
    except Exception as e:
        print(f"An error occurred: {e}")
    

def execute_sql_query(raw_connection, query):
    try:
        cursor = raw_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        print('Query executed')
    except Exception as e:
        print(f"An error occurred: {e}")    
    return results 

  
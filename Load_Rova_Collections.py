import numpy as np
import pandas as pd 
import pymssql
import sqlalchemy
from Data_Labs import *
from pymongo import MongoClient
from prefect import task, flow
from prefect_dask.task_runners import DaskTaskRunner
from datetime import datetime, timedelta
from urllib.parse import quote


def extract_collections(collection_name):
    mongo_client, mongo_db = create_mongodb_connection()
    if collection_name.lower() == 'all':
        all_collections = set(mongo_db.list_collection_names())
        excluded_collections = {'customers', 'sales', 'departments', 'finance', 'products'}
        collections = list(all_collections - excluded_collections)
    else:
        collections = [collection_name]
    return mongo_client, mongo_db, collections


def transform_collections(collection_name, mongo_db):  
    collection_name, nested_data, flat_data = process_collection(collection_name, mongo_db) 
    flat_data = convert_to_sql_compatible_datatypes(flat_data)
    flat_data = handle_duplicate_column_names(flat_data)
    flat_data = flat_data.fillna('')
    flat_data = flat_data.replace('nan', '')  
    #print('flat data transformation completed')
    for key in nested_data.keys():
            nested_data[key] = convert_to_sql_compatible_datatypes(nested_data[key])
            nested_data[key] = handle_duplicate_column_names(nested_data[key])
            nested_data[key] = nested_data[key].fillna('')
            nested_data[key] = nested_data[key].replace('nan', '')
            #print(f'nested data {key} transformation completede')
    return collection_name, flat_data, nested_data             
     

def load_collections(collection_name, flat_data, nested_data, connection, raw_connection):
    if not flat_data.empty:
        flat_table_name = f"{collection_name}"
        flat_data.to_sql(flat_table_name, con=connection, if_exists='replace', index=False)
        stored_procedure = f'sp_convert_varchar_columns_to_indexable_columns [{flat_table_name}]'
        execute_sql_procedure(raw_connection, stored_procedure)
        print(f'{flat_table_name} created') 
        #flat data loaded first then nested data          
    for key, dataframe in nested_data.items():
        if not dataframe.empty:
            nested_table_name = f"{collection_name}_{key}"
            dataframe.to_sql(nested_table_name, con=connection, if_exists='replace', index=False)
            stored_procedure = f'sp_convert_varchar_columns_to_indexable_columns [{nested_table_name}]'
            execute_sql_procedure(connection, stored_procedure)
            print(f'{nested_table_name} created')    
   
         
@task
def mongo_etl_to_sql(collection_name='all'):
    connection = create_sql_connection()
    raw_connection = create_sql_connection(1)
    mongo_client, mongo_db, collections = extract_collections(collection_name)
    for collection_name in collections:
        collection_name, flat_data, nested_data = transform_collections(collection_name, mongo_db)
        load_collections(collection_name, flat_data, nested_data, connection, raw_connection)
    connection.close()
    raw_connection.close()
            

@task
def load_rova_base_tables():
    raw_connection = create_sql_connection(1)
    stored_procedure = 'sp_load_rova_base_tables'
    execute_sql_procedure(raw_connection, stored_procedure)
    raw_connection.close()
  
        
@flow(log_prints=True, task_runner=DaskTaskRunner())
def Load_Rova_Collections():
    tasks = [
        mongo_etl_to_sql.submit('customers'), 
        mongo_etl_to_sql.submit('departments'), 
        mongo_etl_to_sql.submit('sales'),
        mongo_etl_to_sql.submit('products'), 
        mongo_etl_to_sql.submit('finance'),
        mongo_etl_to_sql.submit('all')
    ]
        
    load_rova_base_tables.submit(wait_for=tasks)
    
        
if __name__ == "__main__":
    Load_Rova_Collections.serve(
        name = "Load_Rova_Collections",
        cron = "0 * * * *"
    )  
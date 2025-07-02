'''
=================================================
Milestone 3

Name  : Bagus Rifky Riyanto
Batch : FTDS-027-HCK

This program is created for automation for ETL process. This dataset is about banking customer churn rate.
=================================================
'''

# import libraries
import pandas as pd
import datetime as dt

from datetime import timedelta
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from elasticsearch import Elasticsearch

default_args = {
    'owner': 'Bagus',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
}

with DAG(dag_id='bagus_rifky_DAGS',
         description='Program for ETL process',
         default_args= default_args,
         schedule_interval="10,20,30 09 * * 6",
         catchup = True
         ) as dag:

    start = EmptyOperator(task_id='start')
    

    @task()
    def fetchFromSQL():
        '''

        This function is use for fetching data from SQL, and save the data to csv format
        
        '''
        # database, username, password, and host from docker sql
        database = "airflow"
        username = "airflow"
        password = "airflow"
        host = "postgres"

        # postgres url
        postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
        
        # create engine and connect it
        engine = create_engine(postgres_url)
        conn = engine.connect()

        # fetch daa from sql and save it to csv format
        df = pd.read_sql("select * from table_m3", conn)
        df.to_csv('/opt/airflow/data/data_not_clean.csv', index = False)

    @task()  
    def cleanData():
        '''

        This function is use for data cleaning and preprocessing
        
        '''

        # load data
        df = pd.read_csv('/opt/airflow/data/P2M3_bagus_rifky_data_not_clean.csv')
        # drop column
        df = df.drop(['rownumber'], axis = 1)

        # lowercase column name
        df.columns = df.columns.str.lower()

        # add underscore to columns name
        df.columns = [
        'customer_id', 'surname', 'credit_score', 'country', 
        'gender', 'age', 'tenure', 'balance', 'num_of_products', 'has_cr_card', 
        'is_active_member', 'estimated_salary', 'exited', 'complain', 
        'satisfaction_score', 'card_type', 'point_earned'
        ]

        # drop missing value
        df = df.dropna()
        
        # drop duplicates
        df = df.drop_duplicates()
        
        # change data type for categorical value in number
        df['has_cr_card'] = df['has_cr_card'].astype(str)
        df['is_active_member'] = df['is_active_member'].astype(str)
        df['exited'] = df['exited'].astype(str)
        df['complain'] = df['complain'].astype(str) 
        
        # change values in column 
        df['has_cr_card'] = df['has_cr_card'].replace({'0': 'No', '1': 'Yes'})
        df['is_active_member'] = df['is_active_member'].replace({'0': 'Not Active', '1': 'Active'})
        df['exited'] = df['exited'].replace({'0': 'No', '1': 'Yes'})
        df['complain'] = df['complain'].replace({'0': 'Not Complain', '1': 'Complain'})


        # save to csv
        df.to_csv('/opt/airflow/data/data_clean.csv', index = False)
    
    @task()
    def postElastic():

        '''

        This function is use for posting data to ElasticSearch
        
        '''

        # load data
        df = pd.read_csv('data/data_clean.csv')

        # define elastic search method
        es = Elasticsearch('http://elasticsearch:9200')
        
        # iterating dataframe
        for index, row in df.iterrows():
            res = es.index(index="milestone_data", id=index+1, body=row.to_json())

        return res
    
    end = EmptyOperator(task_id='end')
 

    # prorcess order
    start >> fetchFromSQL() >> cleanData() >> postElastic() >> end
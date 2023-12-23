
#Import airflow Operators and other module
import textwrap
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#Define Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#- Create Script to Execute The Tasks
# Define a Python function to load data into the database
def load_data_to_database(data):
	dataset = f"/home/nantenaina/Documents/10Academy2023/Week02/Challenge2/Data/{data}"
	rawdata = pd.read_csv(dataset)
	rows = len(rawdata)
	max_length = 0
	for i in range (0,922) :
    		text =str(rawdata.iloc[i,0])
    		number = len(text.split(';'))
    		if number > max_length :    
   		 	    max_length = number
	columns = max_length
	
	#create a dataframe with specified delimiter, number of rows and number of columns based on the above information
	dataset = f"/home/nantenaina/Documents/10Academy2023/Week02/Challenge2/Data/{data}"

	rawdata1 = pd.read_csv(dataset, delimiter=';' ,index_col=False, names=range(columns), nrows =rows, low_memory=False)
	#Separate the data into 2 tables 
	track_data = rawdata1.iloc[:, :4]
	trajectory_data= rawdata1.iloc[: , 4:]
	print("Loading data into the database")

# Define the DAG
dag = DAG('data_loading_dag',
          default_args=default_args,
          description='A DAG to load data into a database',
          schedule='@daily',
          catchup=False)

#Instantiate DAG
# BashOperator to create a directory
create_directory = BashOperator(
    task_id='create_directory',
    bash_command='mkdir -p /home/nantenaina/Documents/10Academy2023/Week02/Challenge2/Data',
    dag=dag
)

# PythonOperator to execute Python function (loading data into database)
load_data = PythonOperator(
    task_id='load_data_to_database',
    python_callable=load_data_to_database,
    dag=dag
)

# Set up dependencies
create_directory >> load_data


#Import airflow Operators and other module
import textwrap
import os
import csv
import pandas as pd
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the function to load data into the database
def load_csv_to_postgres(data):
    # PostgreSQL connection details
    conn = psycopg2.connect(
        dbname="Traffic_data",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # CSV file path
    csv_file_path = f"/home/nantenaina/Documents/10Academy2023/Week02/Challenge2/Data/{data}"

    # Open the CSV file and read data
    with open(csv_file_path, 'r', newline='') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row if it exists

        for row in csv_reader:
            # Extract data from CSV rows
            # Assuming column order in the CSV matches the order in the table
            track_id, type, traveled_d, avg_speed, lat, lon, speed, lon_acc, lat_acc, time = row

            # Convert data types if needed
            track_id = int(track_id)
            traveled_d = float(traveled_d)
            avg_speed = float(avg_speed)
            lat = float(lat)
            lon = float(lon)
            speed = float(speed)
            lon_acc = float(lon_acc)
            lat_acc = float(lat_acc)
            

            # Insert data into PostgreSQL dim_track table
            insert_dim_track_query = """
                INSERT INTO dim_track
                (track_id, type, traveled_d, avg_speed)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_dim_track_query, (track_id, type, traveled_d, avg_speed))

            # Insert data into PostgreSQL fact_track table
            insert_fact_track_query = """
                INSERT INTO fact_track
                (track_id, lat, lon, speed, lon_acc, lat_acc, time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_fact_track_query, (track_id, lat, lon, speed, lon_acc, lat_acc, time))

    # Commit changes and close connections
    conn.commit()
    cursor.close()
    conn.close()

# Define the DAG
with DAG('load_data_dag',
         default_args=default_args,
         description='A DAG to load data from CSV to PostgreSQL',
         schedule='@once',
         catchup=False) as dag:

    # Tasks to create database and tables using PostgresOperator
    create_database = SQLExecuteQueryOperator(
        task_id='create_database_task',
        sql="CREATE DATABASE IF NOT EXISTS Traffic_data;",
        postgres_conn_id='Postgres'
    )

    create_dim_track = SQLExecuteQueryOperator(
        task_id='create_dim_track_task',
        sql="""
            CREATE TABLE IF NOT EXISTS dim_track (
                track_id SERIAL PRIMARY KEY,
                type VARCHAR(50),
                traveled_d FLOAT,
                avg_speed FLOAT
            );
        """,
        postgres_conn_id='Postgres'
    )

    create_fact_track = SQLExecuteQueryOperator(
        task_id='create_fact_track_task',
        sql="""
            CREATE TABLE IF NOT EXISTS fact_track (
                id SERIAL PRIMARY KEY,
                track_id INTEGER REFERENCES dim_track(track_id),
                lat FLOAT,
                lon FLOAT,
                speed FLOAT,
                lon_acc FLOAT,
                lat_acc FLOAT,
                time TIMESTAMP
            );
        """,
        postgres_conn_id='Postgres'
    )

    # Task to load data from CSV to PostgreSQL using PythonOperator
    load_data = PythonOperator(
        task_id='load_csv_to_postgres_task',
        python_callable=load_csv_to_postgres,
        dag=dag
    )

    # Set up task dependencies
    create_database >> create_dim_track >> create_fact_track >> load_data

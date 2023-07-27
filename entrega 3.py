import pandas as pd
import os
import psycopg2
import yfinance as yf
from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from psycopg2.extras import execute_values

default_args = {
    'owner': 'Capuzzi',
    'depends_on_past': False,
    'email':['capuzzi022@gmail.com'],
    'email_on_retry': True,
    'email_on_failure': True,
    'start_date': datetime(2023, 7, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


hmy = yf.Ticker('HMY')

    
def importar_data():

 hist = hmy.history(period="1y")
 hist['Date']=hist.index
 hist=hist.reset_index(drop=True)
 hist.to_csv("/opt/airflow/dags/hist.csv")
 
def transformar():
    
 hist=pd.read_csv("/opt/airflow/dags/hist.csv")
 hist= hist.rename(columns={"Open":"Apertura","High":"Precio_alto","Low":"Precio_bajo","Close":"Cierre","Volume":"Volumen","Dividends":"Dividendos","Stock Splits":"Stock","Date":"Dia"})
 hist= hist.drop(columns=["Dividendos","Stock","Volumen"])
 hist.insert(
 loc=0,
 column= "Empresa",
 value="Harmony Gold",
 allow_duplicates=False)
 print(hist)
 Harmony_sin_duplicados= hist.drop_duplicates()#Elimino duplicados
 print(Harmony_sin_duplicados) 
 Harmony_sin_duplicados.to_csv("/opt/airflow/dags/Harmony_sin_duplicados.csv")
 
def Cargar_data():
 env = os.environ
 try:
    conn =psycopg2.connect( host=env['REDSHIFT_HOST'],
                           dbname="data-engineer-database",
                           user= env['REDSHIFT_USER'],
                           password= env['REDSHIFT_PASSWORD'],
                           port="5439"
                          )
    print("me conecte")
 except Exception as e:
    print("odio esto")
    print(e)   
 Harmony_sin_duplicados=pd.read_csv("/opt/airflow/dags/Harmony_sin_duplicados.csv")
 cursor= conn.cursor ()
 table_name="Harmonu_sin_duplicados"
 columns=["Empresa","Apertura","precio_alto","Precio_bajo","Cierre","Dia"]
 values=[tuple(x) for x in Harmony_sin_duplicados.to_numpy()]
 insert_sql= f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
 cursor.execute("BEGIN")
 execute_values(cursor, insert_sql, values)
 cursor.execute("COMMIT")  
 

with DAG (
    'First',
    default_args=default_args,
    
) as dag:
    
  Tarea_1=PythonOperator(task_id='importar_data',python_callable=importar_data)
  Tarea_2=PythonOperator(task_id='transformar',python_callable=transformar)
  Tarea_3=PythonOperator(task_id='Cargar_data',python_callable=Cargar_data)


Tarea_1>>Tarea_2>>Tarea_3




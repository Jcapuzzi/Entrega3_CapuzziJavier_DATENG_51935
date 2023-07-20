# Este es el DAG que orquesta el ETL de la tabla users

import pandas as pd
import os
import json
import logging
import psycopg2
import numpy
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
    'start_date': datetime(2023, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
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
    cursor = conn.cursor()
    cursor.execute(f"""
    create table if not exists {"jcapuzzi_coderhouse"}.Harmony_sin_duplicados (
    Empresa VARCHAR(30),
    Apertura decimal(20,2),
    Precio_alto decimal(20,2),
    Precio_bajo decimal(20,2),
    Cierre decimal(20,2),
    Dia date distkey
) sortkey(Dia);
""")
    conn.commit()
    cursor.close()
    print("Table created!")


   
    
def importar_data():
  hmy = yf.Ticker('HMY')
  hist = hmy.history(period="5y")
  hist['Date']=hist.index
  hist=hist.reset_index(drop=True)
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

  cursor= conn.cursor ()
  table_name="harmony_sin_duplicados"
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



Tarea_1
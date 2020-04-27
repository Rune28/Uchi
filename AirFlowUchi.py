# -*- coding: utf-8 -*-
"""
Created on Mon Apr 27 10:36:04 2020

@author: aovch
"""

import logging

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.operators.clickhouse_operator import ClickHouseOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 4, 25),
    'email': ['alexrune8@gmail.com'],
     'email_on_failure': True,
    'email_on_retry': True,
    
}


sql_t3 = """insert into Dbreport.DateData
    (artist,
      auth,
      firstName
      gender  ,
      itemInSession  ,
      lastName  ,
      length  ,
      level  ,
      location  ,
      method  ,
      page  ,
      registration  ,
      sessionId  ,
      song  ,
      status  ,
      CreatedOn ,
    EventDate ,
      YearWeek,
      YearMonth,
      userAgent,
      userId)
    select   artist,
      auth,
      firstName
      gender  ,
      itemInSession  ,
      lastName  ,
      length  ,
      level  ,
      location  ,
      method  ,
      page  ,
      registration  ,
      sessionId  ,
      song  ,
      status  ,
      CreatedOn toDateTime(ts/1000),
      EventDate toDate(ts/1000),
      YearMonth Uint8 toYYYYmm(ts/1000),
      YearWeek UInt8 toYearWeek(ts/1000),
      userAgent  String,
      userId  String
    from Dbreport.RawData"""


with DAG('ClickHouse update', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    t1 = BashOperator(
        task_id='print_date1',
        bash_command='date',
        dag = dag)

    t2_id = 'Json insert'
    bash_t2 = """cd /home
    docker exec -it home_server_1 bash
    clickhouse-client --query="INSERT INTO Dbreport.RawData FORMAT JSONEachRow" < event-data.json"""
    
    t2 = BashOperator(
        task_id=t2_id,
        bash_command = bash_t2,
        dag = dag
    )
    t3_id = 'ClickHouse Transform'
    t3 = ClickHouseOperator(clickhouse_conn_id = 'clickhouse_default',
        task_id=t3_id,
        sql = sql_t3)

    t4_id = 'RawData truncate'
    t4 = ClickHouseOperator(clickhouse_conn_id = 'clickhouse_default',
        task_id=t4_id,
        sql = 'truncate Dbreport.RawData'
    )

    t1 >> t2 >> t3 >> t4
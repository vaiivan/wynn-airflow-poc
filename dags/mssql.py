#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'email': ['alex.kh.chan@wynnpalace.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def get_dim_metadata_hook():
    list = []
    mssql_hook = MsSqlHook(mssql_conn_id='adex_migration_syn')
    sql = 'SELECT * FROM dbo.ExportDB_Data_ByTableCount'
    rows = mssql_hook.get_records(sql)
    for row in rows:
        list.append(row[2] + "#" + str(row[3]))
    return list


with DAG(
    dag_id = 'adex_scheduler',
    default_args = default_args,
    description = 'Adex Migration Scheduler.',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
    tags=['tian-test'],
) as dag:
    
    get_dim_metadata_task = MsSqlOperator(
        task_id = 'get_dim_metadata_task',
        mssql_conn_id = 'adex_migration_syn',
        sql = r"""
        SELECT 
            *
        FROM 
            dbo.ExportDB_Data_ByTableCount;
        """,

        dag = dag
    )

    get_dim_metadata_hook_task = PythonOperator(
        task_id = 'get_dim_metadata_hook_task',
        python_callable = get_dim_metadata_hook
    )

    get_dim_metadata_task >> get_dim_metadata_hook_task

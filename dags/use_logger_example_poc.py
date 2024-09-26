# %%
import sys
import os
import requests
import numpy as np
#import stopit
#from stopit import threading_timeoutable as timeoutable
import json
import pandas as pd
#sys.path.append("/home/tempadmin/scripts") ###
#from window_config import *
import pymssql
from requests.auth import HTTPBasicAuth
from datetime import timedelta , time, datetime

import logging

import pytz
import time
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

from airflow.utils.db import provide_session
from airflow.models import XCom
from sqlalchemy import func

from kubernetes.client import models as k8s


tz=pytz.timezone("Asia/Hong_Kong")
current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
now = datetime.now(tz).replace(tzinfo=None)
now_time = now.time()

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    dag_id='use_logger_example',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 7, 22, tz="Asia/Hong_Kong"),
    description='use_logger_example',
    catchup=False,
    access_control={'Admin': {'can_read', 'can_edit', 'can_delete'}},
    #DAG timeout setting
    #dagrun_timeout=timedelta(seconds=60)
    concurrency=1,
)


# Define the shared PVC name
#SHARED_PVC_NAME = 'kubeflow-shared-pvc'  # Replace with your actual PVC name
SHARED_PVC_NAME = 'airflow-logs-50gbs'

# Define the mount path inside the container
MOUNT_PATH = '/mnt/shared'

# Add this near the top of your script, after the MOUNT_PATH definition
LOG_DIR = f"{MOUNT_PATH}"

# Create a volume and volume mount for the shared PVC
volume = k8s.V1Volume(
    name='shared-data',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=SHARED_PVC_NAME)
)

volume_mount = k8s.V1VolumeMount(
    name='shared-data',
    mount_path=MOUNT_PATH,
    sub_path=None,
    read_only=False
)


# %%
def get_logger(log_file_name):
    """instance of logger module, will be used for logging operations"""
    
    import logging
    import os
    from pytz import timezone
    from datetime import datetime

    # Ensure the log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Full path to the log file
    log_file_path = os.path.join(LOG_DIR, log_file_name)
    
    # logger config
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Custom formatter with Hong Kong timezone (UTC+8)
    class HKFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            utc_dt = datetime.fromtimestamp(record.created, timezone('UTC'))
            hk_tz = timezone('Asia/Hong_Kong')
            converted = utc_dt.astimezone(hk_tz)
            return converted.strftime('%Y-%m-%d %H:%M:%S')

    # log format
    log_format = HKFormatter("%(levelname)s|%(filename)s:%(lineno)d|%(asctime)s|%(message)s")

    # file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(log_format)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    return logger


def main():
    print('hello world!')
    try:
        print('hello world!')
        logger = get_logger('use_logger_example.log')
        logger.info('test')
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

t1 = PythonOperator(
    task_id='use_logger_example',
    provide_context=True,
    python_callable=main,
    dag=dag,
    max_active_tis_per_dag=1,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[volume_mount],
                        env=[k8s.V1EnvVar(name="LOG_DIR", value=LOG_DIR)]
                    )
                ],
                volumes=[volume],
            )
        )
    }
)


t1
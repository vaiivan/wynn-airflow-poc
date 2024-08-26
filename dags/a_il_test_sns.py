import os 
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.notifications.sns import send_sns_notification


sns_topic_arn = "arn:aws:sns:ap-east-1:695314914535:airflow-sns-tpoic"


dag_failure_sns_notification = send_sns_notification(
    aws_conn_id="aws_connection",
    region_name="ap-east-1",
    message="------- The DAG {{ dag.dag_id }} failed ---------",
    target_arn=sns_topic_arn,
)


with DAG(
    dag_id="a_il_test_sns",
    description="DAG testing for SES",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # on_failure_callback=task_failure_alert,
) as dag:
    a_il_test_fail_5 = PythonOperator(
        task_id="test_sns_fallback",
        python_callable=lambda: 1 / 0,
        on_failure_callback=dag_failure_sns_notification
    )

    a_il_test_fail_5
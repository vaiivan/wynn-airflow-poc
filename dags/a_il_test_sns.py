from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from airflow.providers.amazon.aws.notifications.sns import send_sns_notification


sns_topic_arn = "arn:aws:sns:ap-east-1:695314914535:airflow-sns-tpoic"

time_now = datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d %H:%M:%S')

dag_failure_sns_notification = send_sns_notification(
    aws_conn_id="aws_connection",
    region_name="ap-east-1",
    message="The DAG {{ dag.dag_id }} failed at " + f"{time_now}",
    target_arn=sns_topic_arn,
)


with DAG(
    dag_id="a_il_test_sns",
    description="DAG testing for SES",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_sns_fallback = PythonOperator(
        task_id="test_sns_fallback",
        python_callable=lambda: 1 / 0,
        on_failure_callback=dag_failure_sns_notification
    )

    test_sns_fallback
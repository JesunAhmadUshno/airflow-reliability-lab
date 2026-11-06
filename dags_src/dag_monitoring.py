"""AIR-REL monitoring pipeline: drift statistic into one report row per partition."""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import airrel_common as c

with DAG(
    dag_id="airrel_monitoring",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airrel"],
) as dag:
    prepare = PythonOperator(task_id="prepare", python_callable=c.monitoring_prepare)
    write = PythonOperator(
        task_id="write",
        python_callable=c.monitoring_write,
        retries=1,
        retry_delay=timedelta(seconds=5),
    )
    prepare >> write

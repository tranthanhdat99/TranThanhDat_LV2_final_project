
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from include.config import (
    SPARK_MASTER_URL,
    SPARK_MAX_MEM_PCT,
    SPARK_MIN_WORKERS
)
from include.alert_utils import slack_alert

default_args = {
    "owner": "airflow",
    "on_failure_callback": slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id = "saprk_cluster_monitor",
    default_args = default_args,
    start_date = datetime(2025, 6, 1),
    schedule_interval = "*/5 * * * *",
    catchup = False,
    tags = ["spark", "monitoring"],
) as dag:
    def check_master_up(**context):
        """
        Task 1: Ensure the Spark master API is reachable
        """
        url = f"{SPARK_MASTER_URL}/api/v1/applications"
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
        except Exception as e:
            raise AirflowException(f"Spark Master API unreachable: {e}")
        
    def check_worker_resources(**context):
        """
        Task 2: Fetch master state JSON, 
        ensure enough workers working and no overload in memory usage
        """
        url = f"{SPARK_MASTER_URL}/json/"
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
        except Exception as e:
            raise AirflowException(f"Failed to fetch Spark master state: {e}")
        state = resp.json()
        workers = state.get("workers", [])
        if len(workers) < SPARK_MIN_WORKERS:
            raise AirflowException(f"Only {len(workers)} workers up (<{SPARK_MIN_WORKERS})")
        
        overloaded= []
        for w in workers:
            used = w.get("memoryUsed", 0)
            total = w.get("memory", 0)
            if total and (used/total) >= SPARK_MAX_MEM_PCT:
                overloaded.append(f"{w.get('id')} at {used/total:.0%}")
            
        if overloaded:
            raise AirflowException(f"Worker memory overload: {', '.join(overloaded)}")
    
    t1_master = PythonOperator(
        task_id = "check_master_up",
        python_callable = check_master_up,
    )

    t2_workers = PythonOperator(
        task_id = "check_worker_resources",
        python_callable = check_worker_resources,
    )

    t1_master >> t2_workers




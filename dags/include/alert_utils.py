import requests
from airflow.exceptions import AirflowException
from airflow.models import Variable

from include.config import SLACK_WEBHOOK_VAR

DEFAULT_MESSAGES = {
    #Kafka tasks
    "tcp_health_check": "Kafka connection issue",
    "topic_check": "Kafka topic issue",
    "throughput_check": "Kafka throughput issue",
    "consumer_lag_check": "Kafka lag issue",
     # Spark tasks
    "check_master_up": "Spark Master unreachable",
    "check_worker_resources":  "Spark worker resource issue",
    # Postgres tasks
    "check_recent_ingest": "Postgres ingest issue",
}

def slack_alert(context):
    """
    posts the actual exception message to Slack,
    prefixed by a default per-task message.
    """

    ti = context["task_instance"]
    task = ti.task_id
    dag = ti.dag_id

    exception = context.get('exception') or "Unknown error"

    # Build alert text
    prefix = DEFAULT_MESSAGES.get(task, f"Task {task} failed: ")
    text = f"WARNING: DAG {dag}, Task {task} failed: {prefix}{exception}"

    #Pull webhook URL from Airflow Variable
    webhook = Variable.get("slack_webhook_url", default_var=SLACK_WEBHOOK_VAR)
    if not webhook:
        raise AirflowException("Slack webhook URL  not found in Airflow Variable.")

    #Post to Slack
    resp = requests.post(webhook, json={"text": text})
    if resp.status_code != 200:
        raise AirflowException(f"Failed to send Slack alert, status {resp.status_code}: {resp.text}")
    return resp.text



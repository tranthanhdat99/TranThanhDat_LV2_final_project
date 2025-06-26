from datetime import datetime, timedelta
import socket

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition

from include.alert_utils import slack_alert
from include.config import (
    KAFKA_BROKERS,
    KAFKA_CONF,
    KAFKA_CONSUMER_GROUP_ID,
    KAFKA_LAG_THRESHOLD
)

# FUNCTIONS----------------

def check_tcp_connect(**context):
    down = []
    for host, port in KAFKA_BROKERS:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5s max per broker
        try:
            sock.connect((host, port))
            print(f"{host}:{port} → UP")
        except Exception as e:
            print(f"{host}:{port} → DOWN ({e})")
            down.append(f"{host}:{port}")
        finally:
            sock.close()

    if len(down) == len(KAFKA_BROKERS):
        raise AirflowException(
            f"All Kafka brokers unreachable: {', '.join(down)}"
        )

def check_for_topics(**context):
    p = Producer(KAFKA_CONF)
    try:
        md = p.list_topics(timeout=1000)
    except KafkaError as e:
        raise AirflowException(f"Failed to fetch metadata: {e}")
    #Exclude internal topics
    topic_names = [t for t in md.topics.keys() if not t.startswith("__")]
    if not topic_names:
        raise AirflowException("No user topics found in Kafka cluster.")
    print(f"Found user topics: {topic_names}")
    # store for downstream tasks
    context['ti'].xcom_push(key = 'topic_list', value=topic_names)

def check_throughput(**context):
    topics = context['ti'].xcom_pull(task_ids="topic_check", key="topic_list")
    c_conf = KAFKA_CONF.copy()
    c_conf.update({
        "group.id": KAFKA_CONSUMER_GROUP_ID
    })
    consumer = Consumer(c_conf)
    total_high = 0
    try:
        for t in topics:
            md = consumer.list_topics(topic=t, timeout=1000)
            for pid in md.topics[t].partitions:
                tp = TopicPartition(topic=t, partition=pid)
                _, hi = consumer.get_watermark_offsets(tp, timeout=1000)
                total_high += max(hi, 0)
    
        prev_high = int(Variable.get("kafka_prev_high", default_var="0"))
        if total_high <= prev_high:
            raise AirflowException(f"No new messages: previous={prev_high}, current={total_high}")
    
        Variable.set("kafka_prev_high", str(total_high))
    
    finally:
        consumer.close()

def check_consumer_lag(**context):
    topics = context['ti'].xcom_pull(task_ids="topic_check", key="topic_list")
    c_conf = KAFKA_CONF.copy()
    c_conf.update({
        "group.id": KAFKA_CONSUMER_GROUP_ID
    })
    consumer = Consumer(c_conf)

    total_lag = 0
    for t in topics:
        md = consumer.list_topics(topic=t, timeout=1000)
        for pid in md.topics[t].partitions:
            commited = consumer.commited([(t, pid)], timeout=1)[0].offset
            _, hi = consumer.get_watermark_offsets((t, pid), timeout=1)
            lag = hi - (commited if commited >=0 else 0)
            total_lag += max(lag, 0)

    consumer.close()

    if total_lag > KAFKA_LAG_THRESHOLD:
        raise AirflowException(f"Total consumer lag {total_lag} exceeds threshold {KAFKA_LAG_THRESHOLD}")

# DAG DEFINITION ----------------

default_args = {
    "owner": "airflow",
    "on_failure_callback": slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id = "kafka_full_monitor",
    default_args = default_args,
    description = "Kafka end-to-end monitor: TCP, topics, throughput, and lag",
    start_date = datetime(2025, 6, 1),
    schedule_interval = "*/5 * * * *",
    catchup = False,
    max_active_runs = 1,
    tags = ["kafka", "monitoring"],
) as dag:

    tcp_health_check = PythonOperator(
        task_id = "tcp_health_check",
        python_callable = check_tcp_connect,
        provide_context = True
    )

    topic_check = PythonOperator(
        task_id = "topic_check",
        python_callable = check_for_topics,
        provide_context = True
    )

    throughput_check = PythonOperator(
        task_id = "throughput_check",
        python_callable = check_throughput,
        provide_context = True
    )

    consumer_lag_check = PythonOperator(
        task_id = "consumer_lag_check",
        python_callable = check_consumer_lag,
        provide_context = True
    )

    tcp_health_check >> topic_check >> throughput_check >> consumer_lag_check
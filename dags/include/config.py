
"""
Centralized configuration for all monitoring DAGs (Kafka, Spark, Postgres, etc.).
Values are fetched from Airflow Variables with sensible defaults.
"""
from airflow.models import Variable

# === Helper to parse comma-separated lists ===
def _parse_list(var_name, default):
    raw = Variable.get(var_name, default_var=default)
    return [item.strip() for item in raw.split(",") if item.strip()]

# === Kafka Configuration ===

# Parse brokers from a Variable 'kafka_brokers', defaulting to three brokers
def _parse_brokers(var_name, default):
    entries = _parse_list(var_name, default)
    brokers = []
    for entry in entries:
        host, port = entry.split(":", 1)
        brokers.append((host.strip(), int(port)))
    return brokers

KAFKA_BROKERS = _parse_brokers(
    "kafka_brokers",
    default="kafka-0:9094,kafka-1:9094,kafka-2:9094"
)

KAFKA_CONF = {
    "bootstrap.servers": Variable.get(
        "kafka_bootstrap_servers",
        default_var=",".join(f"{h}:{p}" for h,p in KAFKA_BROKERS)
    ),
    "security.protocol": Variable.get(
        "kafka_security_protocol",
        default_var = "SASL_PLAINTEXT"
    ),
     "sasl.mechanism": Variable.get(
        "kafka_sasl_mechanism",
        default_var = "PLAIN"
    ),
    "sasl.username": Variable.get(
        "kafka_sasl_username",
        default_var = "kafka"
    ),
    "sasl.password": Variable.get(
        "kafka_sasl_password",
        default_var = "UnigapKafka@2024"
    ),
    "socket.timeout.ms": int(Variable.get(
        "kafka_socket_timeout_ms", default_var="1000"
    )),
    "metadata.request.timeout.ms": int(Variable.get(
        "kafka_metadata_timeout_ms", default_var="1000"
    )),
}

KAFKA_CONSUMER_GROUP_ID = Variable.get(
    "kafka_consumer_group_id", default_var="db-writer-group"
)

KAFKA_LAG_THRESHOLD = int(Variable.get(
    "kafka_lag_threshold", default_var="15000"
))

# === Slack Configuration ===
SLACK_WEBHOOK_VAR = Variable.get(
    "slack_webhook_url", 
    default_var="https://hooks.slack.com/services/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"
)

# === Spark Configuration ===
SPARK_MASTER_URL  = Variable.get(
    "spark_master_url", default_var="http://spark:8080"
)
SPARK_MIN_WORKERS = int(Variable.get(
    "spark_min_workers", default_var="1"
))
SPARK_MAX_MEM_PCT = float(Variable.get(
    "spark_max_mem_pct", default_var="0.85"
))

# === Postgres Configuration ===
POSTGRES_CONN_ID = Variable.get(
    "postgres_conn_id", default_var="postgres_default"
)
POSTGRES_INGEST_TABLES = _parse_list(
    "postgres_ingest_tables", default="fact_event"
)


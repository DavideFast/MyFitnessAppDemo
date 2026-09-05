import os

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/bigintensive")
CLICKHOUSE_PROPS = {
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "running_samples")


POSTGRES_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/bigintensive")
POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
}
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "anthropometric_values")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heart-rate-events")
KAFKA_STARTING_OFFSETS = os.getenv("SPARK_STREAM_STARTING_OFFSETS", "latest")

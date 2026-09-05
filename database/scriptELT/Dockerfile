FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Installa dipendenze di sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia lo script ELT e la trasformazione ClickHouse
COPY scriptELT/copyWorkoutToClickhouse.py .
COPY clickhouse/script.sql ./script.sql

# Installa dipendenze Python
RUN pip install --no-cache-dir \
    psycopg[binary]==3.1.14 \
    clickhouse-connect==0.7.3

# Variabili d'ambiente con default
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=bigintensive
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_PORT=8123
ENV CLICKHOUSE_USER=default
ENV CLICKHOUSE_PASSWORD=
ENV CLICKHOUSE_DB=bigintensive

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import psycopg; psycopg.connect(host='${POSTGRES_HOST}', user='${POSTGRES_USER}', password='${POSTGRES_PASSWORD}', dbname='${POSTGRES_DB}')" || exit 1

# Entrypoint
CMD ["python", "copyWorkoutToClickhouse.py"]

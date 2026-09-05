FROM python:3.11-slim

WORKDIR /app

# Installa dipendenze di sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia il generatore di dati PostgreSQL
COPY scriptCreateSamples/createSamplePostgresql.py .

# Installa dipendenze Python
RUN pip install --no-cache-dir \
    Faker==30.8.2 \
    psycopg[binary]==3.1.14

# Variabili d'ambiente con default
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=bigintensive
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import psycopg; psycopg.connect(host='${POSTGRES_HOST}', user='${POSTGRES_USER}', password='${POSTGRES_PASSWORD}', dbname='${POSTGRES_DB}')" || exit 1

# Entrypoint
CMD ["python", "createSamplePostgresql.py"]

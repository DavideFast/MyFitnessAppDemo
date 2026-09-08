# Spark on k3s

This module contains the Spark Python job for runner population analysis and the supporting files to run it on Kubernetes.

## Actual Status

- Runtime Spark: 3.5.3
- Main mode: SparkApplication (Spark Operator) started by the backend
- Namespace: bigintensive
- Job principale: RunningPopolationAnalysis.py

## Essential Structure

```text
data_processing/spark/
├── Dockerfile
├── README.md
├── submit-job.sh
└── jobs/
    ├── RunningPopolationAnalysis.py
    └── config.py
```

## Involved Kubernetes Components

- Manifest Spark/Jupyter: k3s/05-spark-and-jupyter.yaml
- Trigger job dal backend: endpoint /api/v1/startRunningPopulation
- Base Spark configuration in ConfigMap spark-config

## Starting the Analysis Job

The default way to start the analysis job is by calling the backend, which creates a SparkApplication with:

- image: davidefast/bigintensive-sparkwithdependencies:latest
- sparkVersion: 3.5.3
- mainApplicationFile: local:///opt/jobs/RunningPopolationAnalysis.py

Endpoint:

```text
POST /api/v1/startRunningPopulation
```

## Actual Spark Configuration (RunningPopulation job)

Main parameters used by the backend:

- spark.dynamicAllocation.enabled=true
- spark.dynamicAllocation.minExecutors=1
- spark.dynamicAllocation.maxExecutors=4
- spark.executor.cores=2
- spark.executor.memory=2g
- spark.sql.shuffle.partitions=10
- spark.driver.extraClassPath=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar
- spark.executor.extraClassPath=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar

## Build Spark Image

The local Dockerfile prepares a Spark image with:

- job Python in /opt/jobs
- PostgreSQL and ClickHouse JDBC driver
- Python analysis dependencies

Build example:

```bash
cd data_processing/spark
docker build -t davidefast/bigintensive-sparkwithdependencies:latest .
```

## Jupyter (demo)

Jupyter is available in the cluster for notebook exploration and benchmarking.

- Typical URL: http://jupyter.bigintensive.local
- Token configured in the current manifest: bigintensive

Note: the production RunningPopulation job is started by the backend via SparkApplication, it does not depend on Jupyter.

## Data Sources Used by the Job

- ClickHouse (tabella running_samples)
- PostgreSQL (tabella anthropometric_values)

Connection details are centralized in jobs/config.py via environment variables.

## Useful Commands

To delete all running Spark applications in the bigintensive namespace (useful for cleanup crashed or completed jobs):

```bash
kubectl -n bigintensive delete sparkapplication --all
```

FROM apache/spark:3.5.3

USER root

RUN pip install --no-cache-dir delta-spark requests psycopg2-binary numpy matplotlib

RUN mkdir -p /opt/spark/jars
COPY spark/jobs /opt/jobs
ADD https://jdbc.postgresql.org/download/postgresql-42.7.2.jar /opt/spark/jars/postgresql-42.7.2.jar
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar /opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar /opt/spark/jars/kafka-clients-3.6.0.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar

RUN chmod 644 /opt/spark/jars/*.jar

ENV SPARK_CLASSPATH=/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar
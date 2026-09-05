# Stage 1: Build
FROM maven:3.9.5-eclipse-temurin-17 AS builder
WORKDIR /build
COPY kafka-consumer-real-time/ .
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Copy the built JAR
COPY --from=builder /build/target/kafka-consumer-real-time-1.0-SNAPSHOT-jar-with-dependencies.jar /app/kafka-consumer.jar

# Create state store directory
RUN apk add --no-cache libstdc++ libc6-compat
RUN mkdir -p /var/lib/kafka-streams && chmod 777 /var/lib/kafka-streams

# La configurazione arriva dalla ConfigMap e dai Secret del pod (08-kafka-consumer.yaml);
# i valori di default sono nella classe Configurazione.

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD java -version || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=50", "-jar", "/app/kafka-consumer.jar"]


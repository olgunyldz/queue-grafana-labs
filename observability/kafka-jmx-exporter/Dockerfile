# Kafka JMX exporter from the official prometheus/jmx_exporter release (replaces bitnami/jmx-exporter:latest,
# which has no version tags). The jar is pinned by version and verified by its published SHA-256.
# To upgrade: change the version and the checksum (jmx_prometheus_standalone-<version>.jar.sha256 on the
# GitHub release page).
FROM eclipse-temurin:21-jre

# Created first: ADD --chmod would otherwise also apply 644 to the new directory (not traversable).
RUN mkdir -p /opt/jmx-exporter
ADD --checksum=sha256:f62f518d3a396320e4604d6981b6c4f4bd8a4fa46e6200791cf501cb3c6ddd44 --chmod=644 \
    https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_standalone-1.7.0.jar \
    /opt/jmx-exporter/jmx_prometheus_standalone.jar

USER 65534
# Arguments (from compose): <port> <config file>
ENTRYPOINT ["java", "-jar", "/opt/jmx-exporter/jmx_prometheus_standalone.jar"]

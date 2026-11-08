FROM maven:3.9.15-eclipse-temurin-21 AS build

WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B package -DskipTests

FROM flink:2.1.3-java21

COPY --from=build /build/target/flink-reconciler.jar /opt/flink/usrlib/flink-reconciler.jar
COPY conf/flink-conf.yaml /opt/flink/conf/config.yaml
# Job supervisor for the session cluster (see docker/supervise-jobs.sh).
COPY docker/supervise-jobs.sh /opt/flink/docker/supervise-jobs.sh
RUN sed -i 's/\r$//' /opt/flink/docker/supervise-jobs.sh && chmod 755 /opt/flink/docker/supervise-jobs.sh

RUN mkdir -p /opt/flink/checkpoints

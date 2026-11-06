FROM maven:3.9.15-eclipse-temurin-21 AS build

WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B package -DskipTests

FROM flink:2.1.3-java21

COPY --from=build /build/target/fast-flink-reconciler.jar /opt/flink/usrlib/fast-flink-reconciler.jar
COPY conf/flink-conf.yaml /opt/flink/conf/config.yaml

RUN mkdir -p /opt/flink/checkpoints

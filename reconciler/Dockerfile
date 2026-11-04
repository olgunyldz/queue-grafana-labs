# Build context: ./reconciler
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src/ src/
RUN mvn -q -B package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/fast-reconciler.jar /app/fast-reconciler.jar
# RocksDB state lives here; it is rebuilt from the changelog topic if lost.
RUN mkdir -p /var/lib/fast-reconciler && chown -R 1000:1000 /var/lib/fast-reconciler
USER 1000
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
EXPOSE 9400
ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.showDateTime=true", "-Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy/MM/dd HH:mm:ss", "-Dorg.slf4j.simpleLogger.log.org.apache.kafka=warn", "-jar", "/app/fast-reconciler.jar"]

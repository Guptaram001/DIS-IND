FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY pom.xml .
RUN mvn -B -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre

RUN useradd --create-home --uid 10001 disind
WORKDIR /opt/dis-ind

COPY --from=build /workspace/target/dis-ind-1.0.0.jar ./dis-ind.jar
COPY docker/entrypoint.sh ./entrypoint.sh

RUN chmod 0555 ./entrypoint.sh \
    && mkdir -p /data/input /data/output \
    && chown -R disind:disind /opt/dis-ind /data/output

USER disind
EXPOSE 2551

ENTRYPOINT ["/opt/dis-ind/entrypoint.sh"]

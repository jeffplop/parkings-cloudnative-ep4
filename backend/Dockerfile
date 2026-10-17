# ─────────────────────────────────────────────────────────────
# Backend Parkings Together (RabbitMQ) — imagen Docker
# Etapa 1: compila el jar con Maven.  Etapa 2: lo ejecuta con un JRE liviano.
# ─────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -ntp dependency:go-offline
COPY src ./src
RUN mvn -q -ntp -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/parkings-messaging-1.0.0.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]

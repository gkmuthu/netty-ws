FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src
RUN chmod +x mvnw && ./mvnw -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN mkdir -p /app/logs
COPY --from=build /src/target/netty-ws.jar /app/netty-ws.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/netty-ws.jar"]


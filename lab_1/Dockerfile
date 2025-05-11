FROM openjdk:17-jdk-slim
RUN apt-get update && apt-get install -y curl netcat-openbsd iputils-ping net-tools telnet
COPY /build/libs/lab_1-0.0.1-SNAPSHOT.jar /app/lab_1-0.0.1-SNAPSHOT.jar
WORKDIR /app
CMD ["java", "-jar", "lab_1-0.0.1-SNAPSHOT.jar"]
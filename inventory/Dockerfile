FROM amazoncorretto:20-alpine
RUN mkdir /app
COPY app.jar /app/app.jar
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["JAVA", "-jar", "app.jar"]
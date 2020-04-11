FROM maven:3.6.0-jdk-11-slim
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
RUN mvn package
CMD ["mvn", "exec:java", "-D", "exec.mainClass=com.mycompany.app.App"]
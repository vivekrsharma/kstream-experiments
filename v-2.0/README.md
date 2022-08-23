Readme
This is codebase for Java based Producer, Processor and Consumer components using Spring framework.

System Dependencies:
- Apache Maven 3.8.6
- Java version 18.0.2
- Gradle 7.5.1

Pre-requisite:
Currently only producer is implemented using Spring boot and KafkaTemplate.
This runs with an assumption that topics are pre-created in Kafka cluster.

How to run:

Make sure to update config files (application.properties)
with right credentials, server urls etc to point to a specific kafka cluster.

Producer
cd ./producer/
./gradlew build
./gradlew bootRun
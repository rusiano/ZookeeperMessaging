(in construction)

## Requirements

- zookeeper >= 3.4.10
- java >= 1.8
- maven >= 3.7.0

Tested with ubuntu 16.04

## How to use it (script on the way)

1. Download/clone/fork repository and move inside the folder.
2. Compile project with maven `mvn clean install`
3. Launch zookeeper server: i.e. `/opt/zookeeper-3.4.9/bin/zkServer.sh start`
4. Launch kafka server: i.e. `/opt/kafka_2.12-1.0.0/bin/kafka-server-start.sh /opt/kafka_2.12-1.0.0/config/server.properties`
5. Launch master program: i.e. `mvn exec:java -Dexec.mainClass="com.company.Master"`
6. Two different modes:
    1. ZooKeeper Only => Launch workerfactory: i-e. `mvn exec:java -Dexec.mainClass="websocket.WorkerFactory"`
    2. Zookeeper + Kafka streaming => Launch kafkaworkerfactory: i.e. `mvn exec:java -Dexec.mainClass="websocket.KafkaWorkerFactory"`
7. Launch onlineuserkeepalive: i.e. `mvn exec:java -Dexec.mainClass="websocket.OnlineUserKeepAlive"`
8. Access with your favorite browser to the file index.html located in `src/main/java/websocket/index.html`
   - i.e. `firefox src/main/java/websocket/index.html`



## Graphical interface inspired by the following links

[example code of chat using socketio](https://github.com/socketio/chat-example)
[java-server-javascript-client-websockets](https://stackoverflow.com/a/41480670)


### Authors

- [Marcos Bernal](https://github.com/MarcosBernal)
- [Massimiliano Rusiano](https://github.com/rusiano)

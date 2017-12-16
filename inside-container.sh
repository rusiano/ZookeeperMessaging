#! /bin/bash

# Start zookeeper
/opt/zookeeper-3.4.11/bin/zkServer.sh start & > /dev/null

# Start kafka
/opt/kafka_2.11-1.0.0/bin/kafka-server-start.sh /opt/kafka_2.11-1.0.0/config/server.properties &

# Start webserver
/etc/init.d/nginx start &

# Start java processes
cd /home/dev

if [ ! -d "/home/dev/target/dependency" ]; then
  echo ">> Downloading all required dependencies"
  mvn clean install
  mvn dependency:go-offline
fi
mvn exec:java -Dexec.mainClass="websocket.OnlineUserKeepAlive" &
mvn exec:java -Dexec.mainClass="com.company.Master" &
mvn exec:java -Dexec.mainClass="websocket.KafkaWorkerFactory"

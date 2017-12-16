# Zookeeper Messaging use of Kafka, Java, Websockets and Nginx
#
# VERSION               0.1

FROM ubuntu:xenial

USER root

# Install prerequisites
RUN apt-get update
RUN apt-get install -y software-properties-common

# Create folders for adding the files
RUN mkdir -p /home/dev/ /opt/zookeeper-3.4.11 /opt/kafka_2.11-1.0.0 /var/zookeeper

# Install java8
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java8-installer

# Install Zookeeper
RUN cd /opt
RUN wget http://apache.rediris.es/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz
RUN tar -xvf zookeeper-3.4.11.tar.gz -C /opt/
RUN rm  zookeeper-3.4.11.tar.gz

# Install Kafka
RUN wget http://apache.rediris.es/kafka/1.0.0/kafka_2.11-1.0.0.tgz
RUN tar -xvf kafka_2.11-1.0.0.tgz -C /opt/
RUN rm  kafka_2.11-1.0.0.tgz


# Install nginx
RUN apt-get install -y maven nginx

# Add script file that starts all process
# ADD inside-container.sh /home/dev/inside-container.sh
ADD zoo.conf /opt/zookeeper-3.4.11/conf/zoo.cfg
ADD nginx.conf /etc/nginx/nginx.conf

EXPOSE 2181

# Start processes and program Zookeeper (with a script in /home/dev)
CMD /bin/bash

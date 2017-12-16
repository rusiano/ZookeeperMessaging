#! /bin/bash


sudo docker run -it --rm -p 8080:8080 -p 48080:48080 -p 48081:48081 \
       -v $(pwd)/$(dirname $0):/home/dev \
       zookeepermessaging /home/dev/inside-container.sh

#!/bin/bash


docker-compose stop  # Stop the previous instances
docker-compose rm -f  # Remove the stopped instances
docker-compose up -d minio
docker-compose up -d createbuckets
docker-compose up -d zookeeper
docker-compose up -d broker
sleep 15
docker-compose up -d sensorsmock
sleep 5
docker-compose up --build
# to run this chmod +x start.sh
# to run this ./start.sh

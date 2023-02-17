#!/bin/bash

docker compose rm -svf  # Clean up previous instances
docker compose up -d zookeper  
docker compose up -d broker 
docker compose up -d minio
docker compose up -d createbuckets
docker compose up -d --build



#!/bin/bash

docker compose rm -svf  # Clean up previous instances
docker compose up -d broker
docker compose up -d minio
docker compose up -d createbuckets
docker compose up -d rest-proxy
docker compose up -d transformation_service
sleep 30
docker compose up -d --build



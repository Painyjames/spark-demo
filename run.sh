#!/usr/bin/env bash

sbt clean package

docker-compose down

docker rmi -f sparkdemo

docker build -t sparkdemo .

docker-compose up -d

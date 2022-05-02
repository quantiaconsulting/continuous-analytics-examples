#!/bin/bash

# Usage
if [[ -z "$1" ]]; then
  echo "Usage: ./start_consumer_app.sh [id 1|2|3]"
  exit 1
fi

ID=$1

docker exec connect kafka-avro-console-consumer \
   --bootstrap-server kafka1:9092,kafka2:9091 \
   --topic wikipedia.parsed \
   --property schema.registry.url=http://schema-registry:8081 \
   --consumer-property interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor \
   --consumer-property group.id=app \
   --consumer-property client.id=consumer_app_$ID > /dev/null 2>&1 &

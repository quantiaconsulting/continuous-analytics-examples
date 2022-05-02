#!/bin/bash
SCHEMA=$(docker-compose exec schema-registry curl -s -X GET http://schema-registry:8081/subjects/wikipedia.parsed-value/versions/latest | jq .schema)

docker-compose exec schema-registry curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $SCHEMA}" http://schema-registry:8081/subjects/wikipedia.parsed.replica-value/versions

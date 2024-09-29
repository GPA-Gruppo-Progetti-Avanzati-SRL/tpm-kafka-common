#!/bin/bash
/Library/Kafka/bin/kafka-topics.sh --create --if-not-exists --topic echo-out --bootstrap-server localhost:9092
/Library/Kafka/bin/kafka-topics.sh --create --if-not-exists --topic echo-in --bootstrap-server localhost:9092
/Library/Kafka/bin/kafka-topics.sh --create --if-not-exists --topic echo-dlt --bootstrap-server localhost:9092
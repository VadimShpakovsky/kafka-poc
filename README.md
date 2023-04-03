Playground for experiments with Kafka in Python. 

This repo provides basic examples of producing/consuming data using different Python packages. You can can choose which package to use via script parameters, not changing the code.


# Install

```shell
pipenv shell  # activate virtual env
pipenv sync   # install all dependencies
```

# Run Kafka cluster
Live Kafka cluster is required for running all experiments. One of the option is to run cluster in a local Docker container. 

Run Kafka cluster:
```shell
docker-compose -f docker/kafka-zookeeper-dc.yaml up -d
```

Run RedPanda cluster:
```shell
docker-compose -f docker/redpanda-dc.yaml up -d
```

The default Docker Compose config can be setup via `COMPOSE_FILE` environment variable:

```shell
export COMPOSE_FILE=docker/kafka-zookeeper-dc.yaml
```
or put it in `.env` file in the repo root.

# Run Producer
```shell
# list all available Producer implementations
python producer.py list-impls
> aiokafka
> ...

# run Producer using aiokafka implementation
python producer.py produce --impl-name aiokafka
 
```
Infinitely produce data, pushing them to Kafka topic and logging the progress. 

# Run Consumer
```shell
python consumer.py
```
Infinitely consume data, logging consumed data. 
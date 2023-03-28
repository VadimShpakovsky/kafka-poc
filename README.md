Playground for experiments with Kafka in Python.

# Install

```shell
pipenv shell  # activate virtual env
pipenv sync   # install all dependencies
```

# Run Kafka cluster

Run Kafka cluster:
```shell
docker-compose up
```

Run RedPanda cluster:
```shell
docker-compose up -f redpanda-dc.yaml
```

# Run Producer
```shell
python producer.py
```
Infinitely produce data, logging the process. 

# Run Consumer
```shell
python consumer.py
```
Infinitely consume data, logging consumed data. 
from abc import ABC, abstractmethod

from utils.common import join_uri


class BaseConsumer(ABC):
    broker_uri: str
    kafka_topic: str

    def __init__(self, config: dict):
        self.broker_uri = join_uri(config["kafka_server_url"], config["kafka_port"])
        self.kafka_topic = config["kafka_topic"]

    @abstractmethod
    def run(self):
        raise NotImplementedError

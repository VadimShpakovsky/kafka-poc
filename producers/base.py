from abc import ABC, abstractmethod

from utils.common import join_uri
from utils.kafka_utils import setup_kafka


class BaseProducer(ABC):
    def __init__(self, config: dict):
        self.broker_uri = join_uri(config["kafka_server_url"], config["kafka_port"])
        self.kafka_topic = config["kafka_topic"]
        self.produce_delay_sec = config['producer']['produce_delay_sec']

        setup_kafka(
            broker_uri=self.broker_uri,
            topic_name=config["kafka_topic"],
            topic_config=config["topic_config"],
        )

    @abstractmethod
    def run(self):
        raise NotImplementedError

import logging

from kafka import KafkaConsumer

from utils.common import setup_logger
from .base import BaseConsumer

setup_logger()


class PythonKafkaLibConsumer(BaseConsumer):
    kafka_consumer: KafkaConsumer
    group_id: str

    def __init__(self, config: dict):
        super().__init__(config)

        self.group_id = config["consumer"]["group_id"]
        self.kafka_consumer = KafkaConsumer(
            self.kafka_topic, bootstrap_servers=self.broker_uri, group_id=self.group_id
        )

    def run(self):
        logging.info("Start consumer")

        try:
            for message in self.kafka_consumer:
                logging.info(f"Consumed: '{message}'")
        finally:
            self.kafka_consumer.close()

        logging.info("Finish consumer")

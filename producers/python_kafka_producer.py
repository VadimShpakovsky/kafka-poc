import logging
import time
from typing import Generator

from kafka import KafkaProducer

from producers.base import BaseProducer
from utils.common import setup_logger

setup_logger()


class PythonKafkaLibProducer(BaseProducer):
    kafka_producer: KafkaProducer

    def __init__(self, config: dict):
        super().__init__(config)
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_uri)

    def run(self):
        logging.info("Start producer")

        try:
            for message in self._data_gen():
                logging.info(f"Put message to topic: '{message}'")
                self.kafka_producer.send(
                    self.kafka_topic, message.encode("UTF-8"), key="123".encode()
                )
        finally:
            self.kafka_producer.close()

        logging.info("Finish producer")

    def _data_gen(self) -> Generator[str, None, None]:
        counter = 0
        while True:
            yield f"Message #{counter}"
            time.sleep(self.produce_delay_sec)

            counter += 1

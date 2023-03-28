import asyncio
import logging
import time
from typing import AsyncGenerator, Generator

from aiokafka import AIOKafkaProducer
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

from utils.logging_utils import setup_logger

setup_logger()

# Kafka server
KAFKA_SERVER_URL = "localhost:9092"
# Redpanda server
# KAFKA_SERVER_URL = "localhost:19092"

KAFKA_TOPIC = "my_topic"
NUM_PARTITIONS = 2

PRODUCE_DELAY_S = 1


def setup_kafka():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER_URL)

    existed_topics = admin_client.list_topics()
    logging.info(f"Existed topics: {existed_topics}")

    if KAFKA_TOPIC in existed_topics:
        logging.info(f"Topic {KAFKA_TOPIC} already exists.")
    else:
        admin_client.create_topics(
            [
                NewTopic(
                    name=KAFKA_TOPIC,
                    num_partitions=NUM_PARTITIONS,
                    replication_factor=1,
                )
            ]
        )
        logging.info(f"Topic {KAFKA_TOPIC} was created.")


class AsyncProducer:
    producer: AIOKafkaProducer

    def __init__(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVER_URL)
        setup_kafka()

    async def run(self):
        logging.info("Start producer")

        await self.producer.start()
        try:
            async for message in self._data_gen():
                logging.info(f"Put message to topic: '{message}'")
                await self.producer.send_and_wait(
                    KAFKA_TOPIC, message.encode("UTF-8"), key="123".encode()
                )
        finally:
            await self.producer.stop()

        logging.info("Finish producer")

    async def _data_gen(self) -> AsyncGenerator[str, None]:
        counter = 0
        while True:
            yield f"Message #{counter}"
            await asyncio.sleep(PRODUCE_DELAY_S)

            counter += 1


class SyncProducer:
    producer: KafkaProducer

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER_URL)
        # setup_kafka()

    def run(self):
        logging.info("Start producer")

        try:
            for message in self._data_gen():
                logging.info(f"Put message to topic: '{message}'")
                self.producer.send(
                    KAFKA_TOPIC, message.encode("UTF-8"), key="123".encode()
                )
        finally:
            self.producer.close()

        logging.info("Finish producer")

    def _data_gen(self) -> Generator[str, None, None]:
        counter = 0
        while True:
            yield f"Message #{counter}"
            time.sleep(PRODUCE_DELAY_S)

            counter += 1


async def run_async_producer():
    producer = AsyncProducer()
    await producer.run()


def run_sync_producer():
    producer = SyncProducer()
    producer.run()


if __name__ == "__main__":
    # run async producer
    # asyncio.run(run_async_producer())

    # run sync producer
    run_sync_producer()

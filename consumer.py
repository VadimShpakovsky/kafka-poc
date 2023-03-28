import logging

from aiokafka import AIOKafkaConsumer
from kafka import KafkaAdminClient, KafkaConsumer

from utils.logging_utils import setup_logger

setup_logger()

# Kafka server
KAFKA_SERVER_URL = "localhost:9092"
# Redpanda server
# KAFKA_SERVER_URL = "localhost:19092"

PRODUCE_DELAY_S = 1
KAFKA_TOPIC = "my_topic"
CONSUMER_GROUP = "group_1"


async def run():
    verify_kafka()
    await run_consumer()


async def run_consumer():
    logging.info("Start consumer")
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER_URL, group_id=CONSUMER_GROUP
    )

    await consumer.start()
    try:
        async for message in consumer:
            logging.info(f"Consumed: '{message}'")
    finally:
        await consumer.stop()

    logging.info("Finish consumer")


def verify_kafka():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER_URL)

    existed_topics = admin_client.list_topics()

    if KAFKA_TOPIC not in existed_topics:
        raise Exception(f"Topic {KAFKA_TOPIC} doesn't exists.")


class AsyncConsumer:
    consumer: AIOKafkaConsumer

    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER_URL, group_id=CONSUMER_GROUP
        )

    async def run(self):
        logging.info("Start consumer")

        await self.consumer.start()
        try:
            async for message in self.consumer:
                logging.info(f"Consumed: '{message}'")
        finally:
            await self.consumer.stop()

        logging.info("Finish consumer")


class SyncConsumer:
    consumer: KafkaConsumer

    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER_URL, group_id=CONSUMER_GROUP
        )

    def run(self):
        logging.info("Start consumer")

        try:
            for message in self.consumer:
                logging.info(f"Consumed: '{message}'")
        finally:
            self.consumer.close()

        logging.info("Finish consumer")


async def run_async_consumer():
    consumer = AsyncConsumer()
    await consumer.run()


def run_sync_consumer():
    consumer = SyncConsumer()
    consumer.run()


if __name__ == "__main__":
    # run async consumer
    # asyncio.run(run_async_consumer())

    # run sync consumer
    run_sync_consumer()

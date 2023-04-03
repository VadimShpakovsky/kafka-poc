import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from utils.common import setup_logger
from .base import BaseConsumer

setup_logger()


class AioKafkaLibConsumer(BaseConsumer):
    kafka_consumer: AIOKafkaConsumer
    group_id: str

    def __init__(self, config: dict):
        super().__init__(config)

        self.group_id = config["consumer"]["group_id"]

    def run(self):
        asyncio.run(self._async_run())

    async def _async_run(self):
        logging.info("Start consumer")

        self.kafka_consumer = AIOKafkaConsumer(
            self.kafka_topic, bootstrap_servers=self.broker_uri, group_id=self.group_id
        )
        await self.kafka_consumer.start()
        try:
            async for message in self.kafka_consumer:
                logging.info(f"Consumed: '{message}'")
        finally:
            await self.kafka_consumer.stop()

        logging.info("Finish consumer")

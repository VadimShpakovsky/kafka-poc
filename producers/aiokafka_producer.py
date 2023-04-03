import asyncio
import logging
from typing import AsyncGenerator

from aiokafka import AIOKafkaProducer

from producers.base import BaseProducer
from utils.common import setup_logger

setup_logger()


class AioKafkaLibProducer(BaseProducer):
    kafka_producer: AIOKafkaProducer

    def __init__(self, config: dict):
        super().__init__(config)

    def run(self):
        asyncio.run(self._async_run())

    async def _async_run(self):
        logging.info("Start producer")

        kafka_producer = AIOKafkaProducer(bootstrap_servers=self.broker_uri)
        await kafka_producer.start()
        try:
            async for message in self._data_gen():
                logging.info(f"Put message to topic: '{message}'")
                await kafka_producer.send_and_wait(
                    self.kafka_topic, message.encode("UTF-8"), key="123".encode()
                )
        finally:
            await kafka_producer.stop()

        logging.info("Finish producer")

    async def _data_gen(self) -> AsyncGenerator[str, None]:
        counter = 0
        while True:
            yield f"Message #{counter}"
            await asyncio.sleep(self.produce_delay_sec)

            counter += 1

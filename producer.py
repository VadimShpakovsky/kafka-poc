import asyncio
import importlib
import logging
import time
from typing import AsyncGenerator, Generator

import click
import yaml
from aiokafka import AIOKafkaProducer
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

from utils import setup_logger

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


class PythonKafkaProducer:
    kafka_producer: KafkaProducer

    def __init__(self, config: dict):
        self.kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER_URL)
        # setup_kafka()

    def run(self):
        logging.info("Start producer")

        try:
            for message in self._data_gen():
                logging.info(f"Put message to topic: '{message}'")
                self.kafka_producer.send(
                    KAFKA_TOPIC, message.encode("UTF-8"), key="123".encode()
                )
        finally:
            self.kafka_producer.close()

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
    producer = PythonKafkaProducer(None)
    producer.run()


@click.group()
def cli():
    pass


@click.group()
@click.option("--config", "-c", type=click.Path(exists=True), default="config.yaml")
@click.pass_context
def cli(ctx, config):
    with open(config) as f:
        ctx.obj = {"config": yaml.safe_load(f)}


@cli.command()
@click.option("--impl-name", required=True, help="Name of the implementation to use")
@click.pass_context
def produce(ctx, impl_name):
    config = ctx.obj["config"]

    # Parse implementation class name
    try:
        impl_class_name = config["producer"]["implementations"][impl_name]
    except KeyError:
        raise click.BadParameter(
            param_hint="--impl-name",
            message="supported values are provided with list_impls command",
        )

    # Dynamically import the implementation class
    try:
        module = importlib.import_module(__name__)
        impl_class = getattr(module, impl_class_name)
    except (ImportError, AttributeError):
        click.echo(f"ERROR: Could not find implementation class: {impl_class_name}")
        return

    # run producer
    producer = impl_class(config)
    producer.run()


@cli.command()
@click.pass_context
def list_impls(ctx):
    impls = ctx.obj["config"]["producer"]["implementations"]
    for name in impls:
        click.echo(name)


if __name__ == "__main__":
    cli()

    # run async producer
    # asyncio.run(run_async_producer())

    # run sync producer
    # run_sync_producer()

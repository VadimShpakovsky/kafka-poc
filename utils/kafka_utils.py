import logging

from kafka import KafkaAdminClient
from kafka.admin import NewTopic


def setup_kafka(broker_uri: str, topic_name: str, topic_config: dict):
    admin_client = KafkaAdminClient(bootstrap_servers=broker_uri)

    existed_topics = admin_client.list_topics()
    logging.info(f"Existed topics: {existed_topics}")

    if topic_name in existed_topics:
        logging.info(f"Topic {topic_name} already exists.")
    else:
        admin_client.create_topics([NewTopic(name=topic_name, **topic_config)])
        logging.info(f"Topic {topic_name} was created.")

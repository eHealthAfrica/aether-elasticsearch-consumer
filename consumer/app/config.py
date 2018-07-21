import json
import os

consumer_config = None
kafka_config = None


def load_config():
    CONSUMER_CONFIG_PATH = os.environ['ES_CONSUMER_CONFIG_PATH']
    KAFKA_CONFIG_PATH = os.environ['ES_CONSUMER_KAFKA_CONFIG_PATH']
    with open(CONSUMER_CONFIG_PATH) as f:
        global consumer_config
        consumer_config = json.load(f)

    with open(KAFKA_CONFIG_PATH) as f:
        global kafka_config
        kafka_config = json.load(f)


def get_kafka_config():
    return kafka_config


def get_consumer_config():
    return consumer_config


load_config()

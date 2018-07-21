import os
import io
import json
import logging
import sys
import threading
import signal

import spavro
from time import sleep as Sleep
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError, ConflictError
from aet.consumer import KafkaConsumer

from config import get_kafka_config, get_consumer_config

consumer_config = get_consumer_config()
kafka_config = get_kafka_config()

log = logging.getLogger(consumer_config.get('log_name'))
log_level = logging.getLevelName(consumer_config.get('log_level'))
log.setLevel(log_level)

CONN_RETRY = consumer_config.get('startup_connection_retry')
CONN_RETRY_WAIT_TIME = consumer_config.get('connect_retry_wait')

# Global Elasticsearch Connection
es = None


def pprint(obj):
    print(json.dumps(obj, indent=2))


def connect():
    connect_kafka()
    connect_es()


def connect_es():
    for x in range(CONN_RETRY):
        try:
            global es
            # default connection on localhost
            es_urls = consumer_config.get('elasticsearch_instance').get('urls')
            es = Elasticsearch(
                es_urls, sniff_on_start=False)
            print (es.info())
            return
        except Exception:
            print("Could not connect to Elasticsearch Instance")
            Sleep(CONN_RETRY_WAIT_TIME)
    print("Failed to connect to ElasticSearch after %s retries" % CONN_RETRY)
    sys.exit(1)  # Kill consumer with error


def connect_kafka():
    for x in range(CONN_RETRY):
        try:
            consumer = KafkaConsumer(**kafka_config)
            consumer.topics()
            print("Connected to Kafka...")
            return
        except Exception as ke:
            print("Could not connect to Kafka: %s" % (ke))
            Sleep(CONN_RETRY_WAIT_TIME)
    print("Failed to connect to Kafka after %s retries" % CONN_RETRY)
    sys.exit(1)  # Kill consumer with error


class ESConsumerManager(object):

    def __init__(self):
        self.stopped = False
        # SIGTERM should kill subprocess via manager.stop()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.consumer_groups = {}  # index_name : consumer group
        auto_conf = consumer_config.get('autoconfig_settings')
        if auto_conf.get('enabled'):
            self.register_auto_config(**auto_conf)

        #self.load_indices_from_file()

    def load_indices_from_file(self):
        index_path = "%s/index" % FILE_PATH
        if os.path.isdir(index_path):
            index_files = os.listdir(index_path)
            for index_file in index_files:
                self.register_index(index_path=index_path, index_file=index_file)

    def index_from_file(self, index_path, index_file):
        index_name = index_file.split(".")[0]
        path = "%s/%s" % (index_path, index_file)
        with open(path) as f:
            return {
                "name" : index_name,
                "body" : json.load(f)
                }

    def register_auto_config(self, **autoconf):
        log.info({autoconf})


    def register_index(self, index_path=None, index_file=None, index=None):
        if not any([index_path, index_file, index]):
            raise ValueError('Index cannot be created with an artifact')
        if index_path and index_file:
            index = index_from_file(index_path, index_file)

        if es.indices.exists(index=index.get('name')):
            log.debug("Index %s already exists, skipping creation." % index_name)
        else:
            log.info("Creating Index %s" % index.get('name'))
            es.indices.create(index=index_name, body=index.get('body'))
        self.start_consumer_group(index_name, index.get('body'))

    def start_consumer_group(self, index_name, index_body):
        self.consumer_groups[index_name] = ESConsumerGroup(
            index_name, index_body)

    def stop_group(self, index_name):
        self.consumer_groups[index_name].stop()

    def stop(self, *args, **kwargs):
        self.stopped = True
        for key in self.consumer_groups.keys():
            self.stop_group(key)


class ESConsumerGroup(object):
    # Group of consumers (1 per topic) pushing to an ES index

    def __init__(self, index_name, index_body):
        self.name = index_name
        self.consumers = {}
        self.intuit_sources(index_body)

    def intuit_sources(self, index_body):
        for name, instr in index_body.get("mappings", {}).items():
            processor = ESItemProcessor(name, instr)
            self.consumers[processor.topic_name] = ESConsumer(
                self.name, processor)
            self.consumers[processor.topic_name].start()

    def stop(self):
        for name in self.consumers.keys():
            self.consumers[name].stop()


class ESConsumer(threading.Thread):
    # A single consumer subscribed to topic, pushing to an index
    # Runs as a daemon to avoid weird stops
    def __init__(self, index, processor):
        self.processor = processor
        self.index = index
        self.es_type = processor.es_type
        self.topic = processor.topic_name
        self.consumer_timeout = 1000  # MS
        self.consumer_max_records = 1000
        self.group_name = "elastic_%s_%s" % (self.index, self.es_type)
        self.sleep_time = 10
        self.stopped = False
        self.consumer = None
        super(ESConsumer, self).__init__()

    def connect(self):
        args = dict(kafka_config)
        args["group_id"] = self.group_name
        try:
            self.consumer = KafkaConsumer(**args)
            self.consumer.subscribe([self.topic])
            return True
        except Exception as ke:
            log.error("%s failed to subscibe to topic %s with error \n%s" %
                   (self.index, self.topic, ke))
            return False

    def run(self):
        while True:
            if self.connect():
                break
            elif self.stopped:
                return
            Sleep(2)
        last_schema = None
        while not self.stopped:
            new_messages = self.consumer.poll_and_deserialize(
                timeout_ms=self.consumer_timeout,
                max_records=self.consumer_max_records)
            for parition_key, packages in new_messages.items():
                for package in packages:
                    schema = package.get('schema')
                    log.debug("schema: %s" % schema)
                    messages = package.get('messages')
                    log.debug("messages #%s" % len(messages))
                    if schema != last_schema:
                        self.processor.load_avro(schema)
                    for x, msg in enumerate(messages):
                        doc = self.processor.process(msg)
                        log.debug("processed doc %s" % doc)
                        self.submit(doc)
                    last_schema = schema

        log.info("Shutting down consumer %s | %s" % (self.index, self.topic))
        self.consumer.close()
        return

    def submit(self, doc):
        parent = doc.get("_parent", None)
        if parent:  # _parent field can only be in metadata apparently
            del doc['_parent']
        try:
            es.create(
                index=self.index,
                doc_type=self.es_type,
                id=doc.get('id'),
                parent=parent,
                body=doc
            )
        except Exception as ese:
            log.info("Couldn't create doc because of error: %s\nAttempting update." % ese)
            try:
                es.update(
                    index=self.index,
                    doc_type=self.es_type,
                    id=doc.get('id'),
                    parent=parent,
                    body=doc
                )
                log.debug("Success!")
            except TransportError as te:
                log.debug("conflict exists, ignoring document with id %s" %
                      doc.get("id", "unknown"))

    def stop(self):
        log.info ("%s caught stop signal" % (self.group_name))
        self.stopped = True


class ESItemProcessor(object):

    def __init__(self, type_name, type_instructions):
        self.pipeline = []
        self.schema = None
        self.schema_obj = None
        self.es_type = type_name
        self.type_instructions = type_instructions
        self.topic_name = type_name

    def load_avro(self, schema_obj):
        self.schema = spavro.schema.parse(json.dumps(schema_obj))
        self.schema_obj = schema_obj
        self.load()

    def load(self):
        self.pipeline = []
        for key, value in self.type_instructions.items():
            log.debug("process : %s | %s" % (key, value))
            # Check for parent or child instuction and add function:
            #     _add_parent or _add_child to pipeline.
            if key in ["_parent", "_child"]:
                # Given an instuction in the index like, we want to find
                # the appropdiate action to fullfil this properly.
                #
                # "_parent": {
                #     "type": "session"
                # }
                res = {"function": "_add%s" % key}
                # now we need to look for the property matching the value name
                res.update(self._find_matching_predicate(value))
                # we should now have an instuction set with something like:
                # {
                #     "function" : "_add_parent",
                #     "field_name" : "SessionID",
                # }
                self.pipeline.append(res)
            # Look for item named 'location' in properties
            #     If it's there we need a geopoint from _add_geopoint()
            elif key == "properties":
                if "location" in value.keys():
                    res = {"function": "_add_geopoint"}
                    res.update(self._find_geopoints(value))
                    self.pipeline.append(res)

    def process(self, doc, schema=None):
        # Runs the cached insturctions from the built pipeline
        for instr in self.pipeline:
            doc = self.exc(doc, instr)
        return doc

    def exc(self, doc, instr):
        # Excecute by name
        fn = getattr(self, instr.get("function"))
        return fn(doc, **instr)

    def _add_parent(self, doc, field_name=None, **kwargs):
        try:
            doc["_parent"] = self._get_doc_field(doc, field_name)
        except Exception as e:
            log.debug("Could not add parent to doc type %s. Error: %s" %
                   (self.es_type, e))
        return doc

    def _add_child(self, doc, field_name=None, **kwargs):
        try:
            doc["_child"] = self._get_doc_field(doc, field_name)
        except Exception as e:
            log.debug("Could not add parent to doc type %s. Error: %s" %
                   (self.es_type, e))
        return doc

    def _add_geopoint(self, doc, field_name=None, lat=None, lon=None, **kwargs):
        geo = {}
        try:
            geo["lat"] = float(self._get_doc_field(doc, lat))
            geo["lon"] = float(self._get_doc_field(doc, lon))
            doc[field_name] = geo
        except Exception as e:
            log.debug("Could not add geo to doc type %s. Error: %s | %s" %
                   (self.es_type, e, (lat, lon),))
        return doc

    def _get_doc_field(self, doc, name):
        doc = json.loads(json.dumps(doc))
        try:
            # Looks for the key directly
            if name in doc.keys():
                return doc[name]
            else:
                # Looks for Nested Keys
                for key in doc.keys():
                    val = doc.get(key)
                    try:
                        if name in val.keys():
                            return val.get(name)
                    except Exception as err:
                        pass
            raise ValueError()
        except ValueError as ve:
            log.debug("Error getting field %s from doc type %s" %
                   (name, self.es_type))
            log.debug(doc)
            raise ve

    def _find_matching_predicate(self, obj):
        # looks for membership of lowercase name in one of the fields in the schema
        name = obj.get("type")

        for field in self.schema_obj.get("fields"):
            test = field.get("name", "").lower()
            if name.lower() in test:
                return {"field_name": field.get("name")}
        raise ValueError("No matching field found for name %s in type %s | %s" % (
            name, self.es_type, self.schema_obj))

    def _find_geopoints(self, obj):
        res = {"field_name": "location"}
        for field in self.schema_obj.get("fields"):
            test = field.get("name", "").lower()
            if test in ["lat", "latitude"]:
                res["lat"] = field.get("name")
            elif test in ["lon", "lng", "long", "longitude"]:
                res["lon"] = field.get("name")
        if not "lat" and "lon" in res:
            raise ValueError("Couldn't resolve geopoints for field %s of type %s" % (
                "location", self.es_type))
        return res


class KafkaViewer(object):

    def __init__(self):
        self.killed = False
        signal.signal(signal.SIGINT, self.kill)
        signal.signal(signal.SIGTERM, self.kill)
        connect()
        manager = ESConsumerManager()
        log.info("Started ES Consumer")
        while True:
            try:
                pass
                if not manager.stopped:
                    Sleep(10)
                else:
                    log.info("Manager caught SIGTERM, exiting")
                    break
            except KeyboardInterrupt as e:
                log.info("\nTrying to stop gracefully")
                manager.stop()
                break

    def kill(self, *args, **kwargs):
        self.killed = True


if __name__ == "__main__":
    viewer = KafkaViewer()

#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import json
import logging
import sys
import threading
import signal
import http.server
from socketserver import TCPServer
from time import sleep
from urllib3.exceptions import NewConnectionError


from aet.consumer import KafkaConsumer
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.exceptions import ConnectionError as ESConnectionError
import spavro

from . import config

consumer_config = config.get_consumer_config()
kafka_config = config.get_kafka_config()

CONN_RETRY = int(consumer_config.get('startup_connection_retry'))
CONN_RETRY_WAIT_TIME = int(consumer_config.get('connect_retry_wait'))


def get_module_logger(mod_name):
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    level = logging.getLevelName(consumer_config.get('log_level'))
    logger.setLevel(level)
    return logger


log = get_module_logger(consumer_config.get('log_name'))

# Global Elasticsearch Connection
es = None


def connect():
    connect_kafka()
    connect_es()


def connect_es():
    for x in range(CONN_RETRY):
        try:
            global es
            # default connection on localhost
            es_urls = consumer_config.get('elasticsearch_url')
            log.debug('Connecting to ES on %s' % (es_urls,))
            es_connection_info = {
                'port': int(consumer_config.get('elasticsearch_port', 0)),
                'http_auth': consumer_config.get('elasticsearch_http_auth')
            }
            es_connection_info = {k: v for k, v in es_connection_info.items() if v}
            es_connection_info['sniff_on_start'] = False

            es = Elasticsearch(
                es_urls, **es_connection_info)

            log.debug('ES Instance info: %s' % (es.info(),))
            return es
        except NewConnectionError as nce:
            log.debug('Could not connect to Elasticsearch Instance: nce')
            log.debug(nce)
            sleep(CONN_RETRY_WAIT_TIME)
        except ESConnectionError as ese:
            log.debug('Could not connect to Elasticsearch Instance: ese')
            log.debug(ese)
            sleep(CONN_RETRY_WAIT_TIME)

    log.critical('Failed to connect to ElasticSearch after %s retries' % CONN_RETRY)
    sys.exit(1)  # Kill consumer with error


def connect_kafka():
    for x in range(CONN_RETRY):
        try:
            # have to get to force env lookups
            args = kafka_config.copy()
            consumer = KafkaConsumer(**args)
            consumer.topics()
            log.debug('Connected to Kafka...')
            return
        except Exception as ke:
            log.debug('Could not connect to Kafka: %s' % (ke))
            sleep(CONN_RETRY_WAIT_TIME)
    log.critical('Failed to connect to Kafka after %s retries' % CONN_RETRY)
    sys.exit(1)  # Kill consumer with error


class ESConsumerManager(object):

    ES_VERSION_SERIES = 5

    def __init__(self, es_instance):
        self.es = es_instance
        self.stopped = False
        self.autoconfigured_topics = []
        # SIGTERM should kill subprocess via manager.stop()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.serve_healthcheck()
        self.consumer_groups = {}  # index_name : consumer group
        auto_conf = consumer_config.get('autoconfig_settings')
        if auto_conf.get('enabled'):
            self.autoconf_maintainer = AutoConfMaintainer(self, auto_conf)
            self.autoconf_maintainer.start()
        self.load_indices_from_file()

    def load_indices_from_file(self):
        index_path = consumer_config.get('index_path')
        if not index_path:
            log.debug('No valid path for directory of index files')
            return
        if os.path.isdir(index_path):
            index_files = os.listdir(index_path)
            for index_file in index_files:
                self.register_index(index_path=index_path, index_file=index_file)

    def index_from_file(self, index_path, index_file):
        index_name = index_file.split('.')[0]
        path = '%s/%s' % (index_path, index_file)
        with open(path) as f:
            return {
                'name': index_name,
                'body': json.load(f)
            }

    def get_indexes_for_auto_config(self, **autoconf):
        log.debug('Attempting to autoconfigure ES indices')
        ignored_topics = set(autoconf.get('ignored_topics', []))
        log.debug('Ignoring topics: %s' % (ignored_topics,))
        log.debug('Previously Configured topics: %s' % (self.autoconfigured_topics,))
        # have to get to force env lookups
        args = kafka_config.copy()
        try:
            consumer = KafkaConsumer(**args)
            topics = [i for i in consumer.topics()
                      if i not in ignored_topics and
                      i not in self.autoconfigured_topics]
        except Exception as ke:
            log.error('Autoconfig failed to get available topics \n%s' % (ke))
        geo_point = (
            autoconf.get('geo_point_name', None)
            if autoconf.get('geo_point_creation', False)
            else None
        )
        indexes = []
        for topic_name in topics:
            self.autoconfigured_topics.append(topic_name)
            index_name = (autoconf.get('index_name_template') % topic_name).lower()
            log.debug('Index name => %s' % index_name)
            indexes.append(
                {
                    'name': index_name,
                    'body': self.get_index_for_topic(topic_name, geo_point)
                }
            )
        return indexes

    def serve_healthcheck(self):
        self.healthcheck = HealthcheckServer()
        self.healthcheck.start()

    def get_index_for_topic(self, name, geo_point=None):
        log.debug('Auto creating index for topic %s' % name)
        index = {name: {}}
        if geo_point:
            index[name]['_meta'] = {'aet_geopoint': geo_point}
            index[name]['properties'] = {geo_point: {'type': 'geo_point'}}
        log.debug('created index: %s' % index.get('name'))
        return {'mappings': index}

    def register_index(self, index_path=None, index_file=None, index=None):
        if not any([index_path, index_file, index]):
            raise ValueError('Index cannot be created without an artifact')
        try:
            if index_path and index_file:
                log.debug('Loading index %s from file: %s' % (index_file, index_path))
                index = self.index_from_file(index_path, index_file)
            index_name = index.get('name')
            if self.es.indices.exists(index=index.get('name')):
                log.debug('Index %s already exists, skipping creation.' % index_name)
            else:
                log.info('Creating Index %s' % index.get('name'))
                self.es.indices.create(index=index_name, body=index.get('body'))
            self.start_consumer_group(index_name, index.get('body'))
        except Exception as ese:
            log.error('Error creating index in elasticsearch %s' %
                      ([index_path, index_file, index],))
            log.error(ese)
            log.critical("Index not created: path:%s file:%s name:%s" %
                         (index_path, index_file, index.get('name')))

    def start_consumer_group(self, index_name, index_body):
        self.consumer_groups[index_name] = ESConsumerGroup(
            index_name, index_body)

    def stop_group(self, index_name):
        self.consumer_groups[index_name].stop()

    def stop(self, *args, **kwargs):
        self.stopped = True
        self.healthcheck.stop()
        for key in self.consumer_groups.keys():
            self.stop_group(key)


class HealthcheckHandler(http.server.BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        super(HealthcheckHandler, self).__init__(*args, **kwargs)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()


class HealthcheckServer(threading.Thread):

    def __init__(self):
        super(HealthcheckServer, self).__init__()

    def run(self):
        host, port = '0.0.0.0', int(consumer_config.get('consumer_port'))
        handler = HealthcheckHandler
        TCPServer.allow_reuse_address = True
        try:
            self.httpd = TCPServer((host, port), handler)
            self.httpd.serve_forever()
        except OSError as ose:
            log.critical('Could not serve healthcheck endpoint: %s' % ose)
            sys.exit(1)

    def stop(self):
        try:
            log.debug('stopping healthcheck endpoint')
            self.httpd.shutdown()
            self.httpd.server_close()
            log.debug('healthcheck stopped.')
        except AttributeError:
            log.debug('Healthcheck was already down.')


class AutoConfMaintainer(threading.Thread):

    def __init__(self, parent, autoconf):
        self.parent = parent
        self.autoconf = autoconf
        super(AutoConfMaintainer, self).__init__()

    def run(self):
        while not self.parent.stopped:
            auto_indexes = self.parent.get_indexes_for_auto_config(**self.autoconf)
            for idx in auto_indexes:
                self.parent.register_index(index=idx)
            for x in range(10):
                if self.parent.stopped:
                    return
                sleep(1)


class ESConsumerGroup(object):
    # Group of consumers (1 per topic) pushing to an ES index

    def __init__(self, index_name, index_body):
        self.name = index_name
        self.consumers = {}
        log.debug('Consumer Group started for index: %s' % index_name)
        self.intuit_sources(index_body)

    def intuit_sources(self, index_body):
        for name, instr in index_body.get('mappings', {}).items():
            log.debug('Adding processor for %s' % name)
            log.debug('instructions: %s' % instr)
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
    def __init__(self, index, processor, has_group=True):
        # has_group = False only used for testing
        self.processor = processor
        self.index = index
        self.es_type = processor.es_type
        self.topic = processor.topic_name
        self.consumer_timeout = 1000  # MS
        self.consumer_max_records = 1000
        self.group_name = 'elastic_%s_%s' % (self.index, self.es_type) \
            if has_group else None
        self.sleep_time = 10
        self.stopped = False
        self.consumer = None
        super(ESConsumer, self).__init__()

    def connect(self, kafka_config):
        # have to get to force env lookups
        args = kafka_config.copy()
        args['group_id'] = self.group_name
        try:
            self.consumer = KafkaConsumer(**args)
            self.consumer.subscribe([self.topic])
            return True
        except Exception as ke:
            log.error('%s failed to subscibe to topic %s with error \n%s' %
                      (self.index, self.topic, ke))
            return False

    def run(self):
        while True:
            if self.connect(kafka_config):
                break
            elif self.stopped:
                return
            sleep(2)
        last_schema = None
        while not self.stopped:
            new_messages = self.consumer.poll_and_deserialize(
                timeout_ms=self.consumer_timeout,
                max_records=self.consumer_max_records)
            for parition_key, packages in new_messages.items():
                for package in packages:
                    schema = package.get('schema')
                    log.debug('schema: %s' % schema)
                    messages = package.get('messages')
                    log.debug('messages #%s' % len(messages))
                    if schema != last_schema:
                        log.debug('Schema change on type %s' % self.es_type)
                        self.processor.load_avro(schema)
                    else:
                        log.debug('Schema unchanged.')
                    for x, msg in enumerate(messages):
                        doc = self.processor.process(msg)
                        log.debug('processed doc in index %s' % self.es_type)
                        self.submit(doc)
                    last_schema = schema

        log.info('Shutting down consumer %s | %s' % (self.index, self.topic))
        self.consumer.close()
        return

    def submit(self, doc):
        parent = doc.get('_parent', None)
        if parent:  # _parent field can only be in metadata apparently
            del doc['_parent']
        try:
            log.debug('submitting on type %s' % self.es_type)
            es.create(
                index=self.index,
                doc_type=self.es_type,
                id=doc.get('id'),
                parent=parent,
                body=doc
            )
        except Exception as ese:
            log.info('Could not create doc because of error: %s\nAttempting update.' % ese)
            try:
                es.update(
                    index=self.index,
                    doc_type=self.es_type,
                    id=doc.get('id'),
                    parent=parent,
                    body=doc
                )
                log.debug('Success!')
            except TransportError as te:
                log.debug('conflict exists, ignoring document with id %s' %
                          doc.get('id', 'unknown'))

    def stop(self):
        log.info('%s caught stop signal' % (self.group_name))
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
        meta = self.type_instructions.get('_meta')
        if not meta:
            log.debug('type: %s has no meta arguments' % (self.es_type))
            return
        for key, value in meta.items():
            log.debug('Type %s has meta type: %s' % (self.es_type, key))
            if key == 'aet_parent_field':
                cmd = {
                    'function': '_add_parent',
                    'field_name': value
                }
                self.pipeline.append(cmd)
            elif key == 'aet_geopoint':
                cmd = {
                    'function': '_add_geopoint',
                    'field_name': value
                }
                try:
                    cmd.update(self._find_geopoints())
                    self.pipeline.append(cmd)
                except ValueError as ver:
                    log.error('In finding geopoints in pipeline %s : %s' % (self.es_type, ver))
            elif key.startswith('aet'):
                log.error('Unsupported aet _meta keyword %s in type %s' % (key, self.es_type))
            else:
                log.debug('Unknown meta keyword %s in type %s' % (key, self.es_type))
        log.debug('Pipeline for %s: %s' % (self.es_type, self.pipeline))

    def process(self, doc, schema=None):
        # Runs the cached insturctions from the built pipeline
        for instr in self.pipeline:
            doc = self.exc(doc, instr)
        return doc

    def exc(self, doc, instr):
        # Excecute by name
        fn = getattr(self, instr.get('function'))
        return fn(doc, **instr)

    def _add_parent(self, doc, field_name=None, **kwargs):
        try:
            doc['_parent'] = self._get_doc_field(doc, field_name)
        except Exception as e:
            log.error('Could not add parent to doc type %s. Error: %s' %
                      (self.es_type, e))
        return doc

    def _add_geopoint(self, doc, field_name=None, lat=None, lon=None, **kwargs):
        geo = {}
        try:
            geo['lat'] = float(self._get_doc_field(doc, lat))
            geo['lon'] = float(self._get_doc_field(doc, lon))
            doc[field_name] = geo
        except Exception as e:
            log.debug('Could not add geo to doc type %s. Error: %s | %s' %
                      (self.es_type, e, (lat, lon),))
        return doc

    def _get_doc_field(self, doc, name):
        if not name:
            raise ValueError('Invalid field name')
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
            log.debug('Error getting field %s from doc type %s' %
                      (name, self.es_type))
            log.debug(doc)
            raise ve

    def _find_geopoints(self):
        res = {}
        latitude_fields = consumer_config.get('latitude_fields')
        longitude_fields = consumer_config.get('longitude_fields')
        for field in self.schema_obj.get('fields'):
            test = field.get('name', '').lower()
            if test in latitude_fields:
                res['lat'] = field.get('name')
            elif test in longitude_fields:
                res['lon'] = field.get('name')
        if not 'lat' and 'lon' in res:
            raise ValueError('Could not resolve geopoints for field %s of type %s' % (
                'location', self.es_type))
        return res


class ElasticSearchConsumer(object):

    def __init__(self):
        self.killed = False
        signal.signal(signal.SIGINT, self.kill)
        signal.signal(signal.SIGTERM, self.kill)
        log.info('Connecting to Kafka and ES in 5 seconds')
        sleep(5)
        connect()
        manager = ESConsumerManager(es)
        log.info('Started ES Consumer')
        while True:
            try:
                pass
                if not manager.stopped:
                    for x in range(10):
                        sleep(1)
                else:
                    log.info('Manager caught SIGTERM, exiting')
                    break
            except KeyboardInterrupt as e:
                log.info('\nTrying to stop gracefully')
                manager.stop()
                break

    def kill(self, *args, **kwargs):
        self.killed = True


if __name__ == '__main__':
    viewer = ElasticSearchConsumer()

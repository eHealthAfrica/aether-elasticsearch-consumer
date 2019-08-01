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

from datetime import datetime
import json
import os
import signal
import sys
import threading
from time import sleep
from urllib3.exceptions import NewConnectionError

from aet.consumer import KafkaConsumer
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from jsonpath_ng.ext import parse as jsonpath_ng_ext_parse
import spavro

from . import config, healthcheck, index_handler
from .logger import LOG


CONSUMER_CONFIG = config.get_consumer_config()
KAFKA_CONFIG = config.get_kafka_config()

CONN_RETRY = int(CONSUMER_CONFIG.get('startup_connection_retry'))
CONN_RETRY_WAIT_TIME = int(CONSUMER_CONFIG.get('connect_retry_wait'))

# Global Elasticsearch Connection
es = None
ES_VERSION = 0
MT = True  # turn on the magic multi-tenant switch


class CachedParser(object):
    # jsonpath_ng.parse is a very time/compute expensive operation. The output is always
    # the same for a given path. To reduce the number of times parse() is called, we cache
    # all calls to jsonpath_ng here.

    cache = {}

    @staticmethod
    def parse(path):
        # we never need to call parse directly; use find()
        if path not in CachedParser.cache.keys():
            try:
                CachedParser.cache[path] = jsonpath_ng_ext_parse(path)
            except Exception as err:  # jsonpath-ng raises the base exception type
                new_err = 'exception parsing path {path} : {error} '.format(
                    path=path, error=err
                )
                LOG.error(new_err)
                raise err

        return CachedParser.cache[path]

    @staticmethod
    def find(path, obj):
        # find is an optimized call with a potentially cached parse object.
        parser = CachedParser.parse(path)
        return parser.find(obj)


def connect():
    connect_kafka()
    connect_es()


def connect_es():
    for x in range(CONN_RETRY):
        try:
            global es
            # default connection on localhost
            es_urls = [CONSUMER_CONFIG.get('elasticsearch_url')]
            LOG.debug('Connecting to ES on %s' % (es_urls,))

            if CONSUMER_CONFIG.get('elasticsearch_user'):
                http_auth = [
                    CONSUMER_CONFIG.get('elasticsearch_user'),
                    CONSUMER_CONFIG.get('elasticsearch_password')
                ]
            else:
                http_auth = None

            es_connection_info = {
                'port': int(CONSUMER_CONFIG.get('elasticsearch_port', 0)),
                'http_auth': http_auth
            }
            es_connection_info = {k: v for k, v in es_connection_info.items() if v}
            es_connection_info['sniff_on_start'] = False

            es = Elasticsearch(
                es_urls, **es_connection_info)
            es_info = es.info()
            LOG.debug('ES Instance info: %s' % (es_info,))
            LOG.debug(es_info.get('version').get('number'))
            return es
        except (NewConnectionError, ConnectionRefusedError, ESConnectionError) as nce:
            LOG.debug('Could not connect to Elasticsearch Instance: nce')
            LOG.debug(nce)
            sleep(CONN_RETRY_WAIT_TIME)

    LOG.critical('Failed to connect to ElasticSearch after %s retries' % CONN_RETRY)
    sys.exit(1)  # Kill consumer with error


def connect_kafka():
    for x in range(CONN_RETRY):
        try:
            # have to get to force env lookups
            args = KAFKA_CONFIG.copy()
            consumer = KafkaConsumer(**args)
            consumer.topics()
            LOG.debug('Connected to Kafka...')
            return
        except Exception as ke:
            LOG.debug('Could not connect to Kafka: %s' % (ke))
            sleep(CONN_RETRY_WAIT_TIME)
    LOG.critical('Failed to connect to Kafka after %s retries' % CONN_RETRY)
    sys.exit(1)  # Kill consumer with error


class ESConsumerManager(object):

    SUPPORTED_ES_SERIES = [5, 6, 7]

    def __init__(self, es_instance):
        self.es = es_instance
        self.stopped = False
        self.autoconfigured_topics = []
        # SIGTERM should kill subprocess via manager.stop()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.serve_healthcheck()
        self.es_version = self.get_es_version()
        self.consumer_groups = {}  # index_name : consumer group
        auto_conf = CONSUMER_CONFIG.get('autoconfig_settings', {})
        self.autoconf_maintainer = AutoConfMaintainer(self, auto_conf)
        self.autoconf_maintainer.start()
        self.load_indices_from_file()

    def serve_healthcheck(self):
        self.healthcheck = healthcheck.HealthcheckServer()
        self.healthcheck.start()

    def load_indices_from_file(self):
        index_path = CONSUMER_CONFIG.get('index_path')
        if not index_path:
            LOG.debug('No valid path for directory of index files')
            return
        if os.path.isdir(index_path):
            index_files = os.listdir(index_path)
            for index_file in index_files:
                self.register_index(index_path=index_path, index_file=index_file)

    def get_es_version(self):
        info = self.es.info()
        version = info.get('version').get('number')
        series = int(version.split('.')[0])
        if series not in ESConsumerManager.SUPPORTED_ES_SERIES:
            raise ValueError('Version : %s is not supported' % version)
        global ES_VERSION
        ES_VERSION = series
        return series

    def register_index(self, index_path=None, index_file=None, index=None):
        try:
            index_handler.register_es_index(
                self.es, index_path, index_file, index
            )
            self.start_consumer_group(
                index.get('name'),
                index.get('body')
            )
        except Exception as ese:
            LOG.error('Error creating index in elasticsearch %s' %
                      ([index_path, index_file, index],))
            LOG.error(ese)
            LOG.critical('Index not created: path:%s file:%s name:%s' %
                         (index_path, index_file, index.get('name')))

    def start_consumer_group(self, index_name, index_body):
        if self.es_version == 5:
            self.consumer_groups[index_name] = ESConsumerGroupV5(
                index_name, index_body)
        else:
            self.consumer_groups[index_name] = ESConsumerGroup(
                index_name, index_body)

    def stop_group(self, index_name):
        self.consumer_groups[index_name].stop()

    def stop(self, *args, **kwargs):
        self.stopped = True
        self.healthcheck.stop()
        for key in self.consumer_groups.keys():
            self.stop_group(key)


class AutoConfMaintainer(threading.Thread):

    def __init__(self, parent, autoconf=None):
        LOG.debug('Started Autoconf Maintainer')
        self.parent = parent
        self.autoconf = autoconf
        self.configured_topics = []
        self.kibana_index = None
        self.consumer = KafkaConsumer(**KAFKA_CONFIG.copy())
        if self.autoconf.get('create_kibana_index', True):
            kibana_index = self.autoconf.get('index_name_template')
            self.kibana_index = kibana_index.split('%s')[0]
        super(AutoConfMaintainer, self).__init__()

    def run(self):
        # On start
        while not self.parent.stopped:
            # Check autoconfig
            if not self.autoconf.get('enabled', False):
                LOG.debug('Autoconf disabled.')
            if self.autoconf.get('enabled', False):
                LOG.debug('Autoconfig checking for new indices')
                LOG.debug(
                    f'Previously Configured topics: {self.configured_topics}'
                )
                new_topics = self.find_new_topics()
                if not new_topics:
                    LOG.debug('No new topics')
                for topic in new_topics:
                    LOG.debug(
                        f'New topic: {topic}'
                    )
                    index = index_handler.get_es_index_from_autoconfig(
                        self.autoconf,
                        topic,
                        MT
                    )
                    self.parent.register_index(index=index)
                    self.configured_topics.append(topic)

            # Check running threads
            try:
                self.check_running_groups()
            except Exception as err:
                LOG.error(f'Error watching running threads: {err}')
            for x in range(10):
                if self.parent.stopped:
                    return
                sleep(1)

    def find_new_topics(self):
        ignored_topics = set(self.autoconf.get('ignored_topics', []))
        try:
            topics = [i for i in self.consumer.topics()
                      if i not in ignored_topics and
                      i not in self.configured_topics]
            return topics
        except Exception as ke:
            LOG.error('Autoconfig failed to get available topics \n%s' % (ke))
            return []  # Can't auto-configure if Kafka isn't available

    def check_running_groups(self):
        groups = self.parent.consumer_groups
        for k, group in groups.items():
            group.monitor_threads()


class ESConsumerGroup(object):
    # Group of consumers (1 per topic) pushing to an ES index

    def __init__(self, index_name, index_body):
        self.name = index_name
        self.consumers = {}
        self.topics = {}  # configuration for each topic
        LOG.debug('Consumer Group started for index: %s' % index_name)
        self.intuit_sources(index_body)

    def intuit_sources(self, index_body):
        for doc_type, instr in index_body.get('mappings', {}).items():
            # There's only one type per mapping allowed in ES6
            LOG.debug('Adding processor for %s' % doc_type)
            LOG.debug('instructions: %s' % instr)
            topics = instr.get('_meta').get('aet_subscribed_topics')
            if not topics:
                raise ValueError('No topics in aet_subscribed_topics section in index %s' %
                                 self.name)
            for topic in topics:
                self.start_topic(topic, doc_type, instr)

    def start_topic(self, topic_name, doc_type=None, instr=None, item_processor='V6'):
        if item_processor == 'V5':
            ItemProcessor = ESItemProcessorV5
        else:
            ItemProcessor = ESItemProcessor
        LOG.debug(f'Group: {self.name} is starting topic: {topic_name}')
        if instr is not None:  # can be {}
            self.topics[topic_name] = (topic_name, doc_type, instr)
        try:
            topic_name, doc_type, instr = self.topics[topic_name]
        except KeyError as ker:
            LOG.error(ker)
            raise ValueError(f'Topic {topic_name} on group {self.name} has no instructions.')
        processor = ItemProcessor(topic_name, instr)
        self.consumers[topic_name] = ESConsumer(self.name, processor, doc_type=doc_type)
        self.consumers[topic_name].start()

    def is_alive(self, topic_name):
        try:
            return self.consumers[topic_name].is_alive()
        except KeyError as ker:
            LOG.error(f'Error getting liveness on {self.name}:{topic_name}: {ker}')
            return False

    def monitor_threads(self):
        LOG.debug(f'Checking threads on group: {self.name}')
        for topic in self.consumers.keys():
            if not self.is_alive(topic):
                LOG.error(f'Topic {topic} on group {self.name} died. Restarting.')
                self.start_topic(topic)

    def stop(self):
        for name in self.consumers.keys():
            self.consumers[name].stop()


class ESConsumerGroupV5(ESConsumerGroup):
    # Same as Normal Group, just uses the V5 item processor
    # and doc_type is None
    def intuit_sources(self, index_body):
        for name, instr in index_body.get('mappings', {}).items():
            # There's only one type per mapping allowed in ES6.
            # ES5 implementation is simpler.
            LOG.debug('Adding processor for %s' % name)
            LOG.debug('instructions: %s' % instr)
            self.start_topic(name, None, instr, 'V5')


class ESConsumer(threading.Thread):
    # A single consumer subscribed to topic, pushing to an index
    # Runs as a daemon to avoid weird stops
    def __init__(self, index, processor, has_group=True, doc_type=None):
        # has_group = False only used for testing
        self.processor = processor
        self.doc_type = doc_type
        self.es_type = processor.es_type
        self.topic = processor.topic_name
        if MT:
            self.tenant = self.topic.split('.')[0]
        self.index = index
        self.consumer_timeout = 8000  # MS
        self.consumer_max_records = 1000
        kafka_topic_template = CONSUMER_CONFIG.get(
            'kafka_topic_template',
            'elastic_{es_index_name}_{data_type}'
        )
        if MT:
            idx_comp = kafka_topic_template.format(
                es_index_name=self.index,
                data_type=self.es_type)
            self.group_name = f'{self.tenant}.{idx_comp}'
        else:
            self.group_name = kafka_topic_template.format(
                es_index_name=self.index,
                data_type=self.es_type)\
                if has_group else None
        self.sleep_time = 10
        self.stopped = False
        self.consumer = None
        self.thread_id = 0
        super(ESConsumer, self).__init__()

    def connect(self):
        # have to get to force env lookups
        args = KAFKA_CONFIG.copy()
        args['client_id'] = self.group_name
        args['group_id'] = self.group_name
        try:
            LOG.debug(
                f'Kafka CONFIG [{self.thread_id}]'
                f'[{self.index}:{self.group_name}]')
            LOG.debug(json.dumps(args, indent=2))
            self.consumer = KafkaConsumer(**args)
            self.consumer.subscribe([self.topic])
            LOG.debug('Consumer %s subscribed on topic: %s @ group %s' %
                      (self.index, self.topic, self.group_name))
            return True
        except Exception as ke:
            LOG.error('%s failed to subscibe to topic %s with error \n%s' %
                      (self.index, self.topic, ke))
            return False

    def run(self):
        self.thread_id = threading.get_ident()
        LOG.debug(f'Consumer [{self.thread_id}] running on {self.index} : {self.es_type}')
        while True:
            if self.connect():
                break
            elif self.stopped:
                return
            sleep(2)
        last_schema = None
        while not self.stopped:
            new_messages = self.consumer.poll_and_deserialize(
                timeout_ms=self.consumer_timeout,
                max_records=self.consumer_max_records)
            if not new_messages:
                LOG.info(
                    f'Kafka IDLE [{self.thread_id}]'
                    f'[{self.index}:{self.group_name}]')
                sleep(5)
                continue
            for parition_key, packages in new_messages.items():
                LOG.debug(f'read PK: {parition_key}')
                for package in packages:
                    schema = package.get('schema')
                    messages = package.get('messages')
                    LOG.debug('messages #%s' % len(messages))
                    if schema != last_schema:
                        LOG.info('Schema change on type %s' % self.es_type)
                        LOG.debug('schema: %s' % schema)
                        self.processor.load_avro(schema)
                        self.get_route = self.processor.create_route()
                    else:
                        LOG.debug('Schema unchanged.')
                    count = 0
                    for x, msg in enumerate(messages):
                        doc = self.processor.process(msg)
                        count = x
                        LOG.debug(
                            f'Kafka READ [{self.thread_id}]'
                            f'[{self.index}:{self.group_name}]'
                            f' -> {doc.get("id")}')
                        self.submit(doc)
                    LOG.debug(
                        f'Kafka COMMIT [{self.thread_id}]'
                        f'[{self.index}:{self.group_name}]')
                    self.consumer.commit_async(callback=self.report_commit)
                    LOG.info('processed %s docs in index %s' % ((count + 1), self.es_type))
                    last_schema = schema

        LOG.info('Shutting down consumer %s | %s' % (self.index, self.topic))
        self.consumer.close(autocommit=True)
        return

    def report_commit(self, offsets, response):
        LOG.info(f'Kafka OFFSET CMT {offsets} -> {response}')

    def submit(self, doc, route=None):
        parent = doc.get('_parent', None)
        if parent:  # _parent field can only be in metadata apparently
            del doc['_parent']
        try:
            if ES_VERSION > 5:
                route = self.get_route(doc)
                '''
                if route:
                    LOG.debug(doc)
                    LOG.debug(route)
                '''
                es.create(
                    index=self.index,
                    id=doc.get('id'),
                    routing=route,
                    doc_type=self.doc_type,
                    body=doc
                )
            else:
                es.create(
                    index=self.index,
                    doc_type=self.es_type,
                    id=doc.get('id'),
                    parent=parent,
                    body=doc
                )
            LOG.debug(
                f'ES CREATE-OK [{self.thread_id}]'
                f'[{self.index}:{self.group_name}]'
                f' -> {doc.get("id")}')

        except (Exception, TransportError) as ese:
            LOG.info('Could not create doc because of error: %s\nAttempting update.' % ese)
            try:
                if ES_VERSION > 5:
                    route = self.get_route(doc)
                    es.update(
                        index=self.index,
                        id=doc.get('id'),
                        routing=route,
                        doc_type=self.doc_type,
                        body=doc
                    )
                else:
                    es.update(
                        index=self.index,
                        doc_type=self.es_type,
                        id=doc.get('id'),
                        parent=parent,
                        body=doc
                    )
                LOG.debug(
                    f'ES UPDATE-OK [{self.thread_id}]'
                    f'[{self.index}:{self.group_name}]'
                    f' -> {doc.get("id")}')
            except TransportError:
                LOG.debug('conflict exists, ignoring document with id %s' %
                          doc.get('id', 'unknown'))

    def stop(self):
        LOG.info('%s caught stop signal' % (self.group_name))
        self.stopped = True


class ESItemProcessor(object):

    def __init__(self, type_name, type_instructions):
        self.pipeline = []
        self.schema = None
        self.schema_obj = None
        self.es_type = type_name
        self.type_instructions = type_instructions
        self.topic_name = type_name
        self.has_parent = False

    def load_avro(self, schema_obj):
        self.schema = spavro.schema.parse(json.dumps(schema_obj))
        self.schema_obj = schema_obj
        self.load()

    def load(self):
        self.pipeline = []
        self.has_parent = False
        meta = self.type_instructions.get('_meta')
        if not meta:
            LOG.debug('type: %s has no meta arguments' % (self.es_type))
            return
        for key, value in meta.items():
            LOG.debug('Type %s has meta type: %s' % (self.es_type, key))
            if key == 'aet_parent_field':
                join_field = meta.get('aet_join_field', None)
                if join_field:  # ES 6
                    field = value.get(self.es_type)
                else:  # ES 5
                    field = value
                if field and join_field:
                    self.has_parent = True
                    cmd = {
                        'function': '_add_parent',
                        'field_name': field,
                        'join_field': join_field
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
                    LOG.error('In finding geopoints in pipeline %s : %s' % (self.es_type, ver))
            elif key == 'aet_auto_ts':
                cmd = {
                    'function': '_add_timestamp',
                    'field_name': value
                }
                self.pipeline.append(cmd)
            elif key.startswith('aet'):
                LOG.debug('aet _meta keyword %s in type %s generates no command'
                          % (key, self.es_type))
            else:
                LOG.debug('Unknown meta keyword %s in type %s' % (key, self.es_type))
        LOG.debug('Pipeline for %s: %s' % (self.es_type, self.pipeline))

    def create_route(self):
        meta = self.type_instructions.get('_meta', {})
        join_field = meta.get('aet_join_field', None)
        if not self.has_parent or not join_field:
            LOG.debug('NO Routing created for type %s' % self.es_type)
            return lambda *args: None

        def route(doc):
            return doc.get(join_field).get('parent')
        LOG.debug('Routing created for child type %s' % self.es_type)
        return route

    def rename_reserved_fields(self, doc):
        reserved = ['_uid', '_id', '_type', '_source', '_all', '_field_names',
                    '_routing', '_index', '_size', '_timestamp', '_ttl', '_version']
        for key in doc:
            if key in reserved:
                val = self._get_doc_field(doc, key)
                safe_name = 'es_reserved_%s' % key
                doc[safe_name] = val
                del doc[key]
        return doc

    def process(self, doc, schema=None):
        # Runs the cached insturctions from the built pipeline
        for instr in self.pipeline:
            doc = self.exc(doc, instr)
        doc = self.rename_reserved_fields(doc)
        return doc

    def exc(self, doc, instr):
        # Excecute by name
        fn = getattr(self, instr.get('function'))
        return fn(doc, **instr)

    def _add_parent(self, doc, field_name=None, join_field=None, **kwargs):
        try:
            payload = {
                'name': self.es_type,
                'parent': self._get_doc_field(doc, field_name)
            }
            doc[join_field] = payload
        except Exception as e:
            LOG.error('Could not add parent to doc type %s. Error: %s' %
                      (self.es_type, e))
        return doc

    def _add_geopoint(self, doc, field_name=None, lat=None, lon=None, **kwargs):
        geo = {}
        try:
            geo['lat'] = float(self._get_doc_field(doc, lat))
            geo['lon'] = float(self._get_doc_field(doc, lon))
            doc[field_name] = geo
        except Exception as e:
            LOG.debug('Could not add geo to doc type %s. Error: %s | %s' %
                      (self.es_type, e, (lat, lon),))
        return doc

    def _add_timestamp(self, doc, field_name=None, **kwargs):
        doc[field_name] = str(datetime.now().isoformat())
        return doc

    def _get_doc_field(self, doc, name):
        if not name:
            raise ValueError('Invalid field name')
        doc = json.loads(json.dumps(doc))
        try:
            matches = CachedParser.find(name, doc)
            if not matches:
                raise ValueError(name, doc)
            if len(matches) > 1:
                LOG.warn('More than one value for %s in doc type %s, using first' %
                         (name, self.es_type))
            return matches[0].value
        except ValueError as ve:
            LOG.debug('Error getting field %s from doc type %s' %
                      (name, self.es_type))
            LOG.debug(doc)
            raise ve

    def _find_geopoints(self):
        res = {}
        latitude_fields = CONSUMER_CONFIG.get('latitude_fields')
        longitude_fields = CONSUMER_CONFIG.get('longitude_fields')
        LOG.debug(f'looking for matches in {latitude_fields} & {longitude_fields}')
        for lat in latitude_fields:
            path = self.find_path_in_schema(self.schema_obj, lat)
            if path:
                res['lat'] = path[0]  # Take the first Lat
                break
        for lng in longitude_fields:
            path = self.find_path_in_schema(self.schema_obj, lng)
            if path:
                res['lon'] = path[0]  # Take the first Lng
                break
        if 'lat' not in res or 'lon' not in res:
            raise ValueError('Could not resolve geopoints for field %s of type %s' % (
                'location', self.es_type))
        return res

    def find_path_in_schema(self, schema, test, previous_path='$'):
        # Searches a schema document for matching instances of an element.
        # Will look in nested objects. Aggregates matching paths.
        LOG.debug(f'search: {test}:{previous_path}')
        matches = []
        if isinstance(schema, list):
            for _dict in schema:
                types = _dict.get('type')
                if not isinstance(types, list):
                    types = [types]  # treat everything as a union
                for _type in types:
                    if not _type:  # ignore nulls in unions
                        continue
                    if isinstance(_type, dict):
                        name = _dict.get('name')
                        next_level = _type.get('fields')
                        if next_level:
                            matches.extend(
                                self.find_path_in_schema(
                                    next_level,
                                    test,
                                    f'{previous_path}.{name}'
                                )
                            )
                    test_name = _dict.get('name', '').lower()
                    if str(test_name) == str(test.lower()):
                        return [f'{previous_path}.{test_name}']
        elif schema.get('fields'):
            matches.extend(
                self.find_path_in_schema(schema.get('fields'), test, previous_path)
            )
        return [m for m in matches if m]


class ESItemProcessorV5(ESItemProcessor):

    def load(self):
        self.pipeline = []
        meta = self.type_instructions.get('_meta')
        if not meta:
            LOG.debug('type: %s has no meta arguments' % (self.es_type))
            return
        for key, value in meta.items():
            LOG.debug('Type %s has meta type: %s' % (self.es_type, key))
            if key == 'aet_parent_field':
                cmd = {
                    'function': '_add_parent',
                    'field_name': value
                }
                self.pipeline.append(cmd)
                LOG.debug('Added %s to pipeline' % cmd)
            elif key == 'aet_geopoint':
                cmd = {
                    'function': '_add_geopoint',
                    'field_name': value
                }
                try:
                    cmd.update(self._find_geopoints())
                    self.pipeline.append(cmd)
                    LOG.debug('Added %s to pipeline' % cmd)
                except ValueError as ver:
                    LOG.error('In finding geopoints in pipeline %s : %s' % (self.es_type, ver))
            elif key.startswith('aet'):
                LOG.debug('aet _meta keyword %s in type %s generates no command'
                          % (key, self.es_type))
            else:
                LOG.debug('Unknown meta keyword %s in type %s' % (key, self.es_type))
        LOG.debug('Pipeline for %s: %s' % (self.es_type, self.pipeline))

    def _add_parent(self, doc, field_name=None, **kwargs):
        try:
            doc['_parent'] = self._get_doc_field(doc, field_name)
        except Exception as e:
            LOG.error('Could not add parent to doc type %s. Error: %s' %
                      (self.es_type, e))
        return doc


class ElasticSearchConsumer(object):

    def __init__(self):
        self.killed = False
        signal.signal(signal.SIGINT, self.kill)
        signal.signal(signal.SIGTERM, self.kill)
        LOG.info('Connecting to Kafka and ES in 5 seconds')
        # sleep(5)
        connect()
        manager = ESConsumerManager(es)
        LOG.info('Started ES Consumer')
        while True:
            try:
                if not manager.stopped:
                    for x in range(10):
                        sleep(1)
                else:
                    LOG.info('Manager caught SIGTERM, exiting')
                    break
            except KeyboardInterrupt:
                LOG.info('\nTrying to stop gracefully')
                manager.stop()
                break

    def kill(self, *args, **kwargs):
        self.killed = True


if __name__ == '__main__':
    viewer = ElasticSearchConsumer()

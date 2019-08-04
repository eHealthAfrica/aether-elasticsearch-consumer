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

import json
import re
import signal
import sys
import threading
from time import sleep
from urllib3.exceptions import NewConnectionError

from aet.consumer import KafkaConsumer
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.exceptions import ConnectionError as ESConnectionError

from . import config, healthcheck, index_handler
from .logger import LOG
from processor import ESItemProcessor


CONSUMER_CONFIG = config.get_consumer_config()
KAFKA_CONFIG = config.get_kafka_config()

CONN_RETRY = int(CONSUMER_CONFIG.get('startup_connection_retry'))
CONN_RETRY_WAIT_TIME = int(CONSUMER_CONFIG.get('connect_retry_wait'))

# Global Elasticsearch Connection
es = None
ES_VERSION = 0
MT = CONSUMER_CONFIG.get('multi-tenant', True)


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

    SUPPORTED_ES_SERIES = [6, 7]

    def __init__(self, es_instance):
        self.es = es_instance
        self.stopped = False
        self.autoconfigured_topics = []
        # SIGTERM should kill subprocess via manager.stop()
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.serve_healthcheck()
        self.es_version = self.get_es_version()
        self.groups = {}  # index_name : consumer group
        auto_conf = CONSUMER_CONFIG.get('autoconfig_settings', {})
        self.autoconf_maintainer = AutoConfMaintainer(self, auto_conf)
        self.autoconf_maintainer.start()

    def serve_healthcheck(self):
        self.healthcheck = healthcheck.HealthcheckServer()
        self.healthcheck.start()

    def get_es_version(self):
        info = self.es.info()
        version = info.get('version').get('number')
        series = int(version.split('.')[0])
        if series not in ESConsumerManager.SUPPORTED_ES_SERIES:
            raise ValueError('Version : %s is not supported' % version)
        global ES_VERSION
        ES_VERSION = series
        return series

    def notify_topic(self, tenant, topic):
        self.start_group(tenant)
        LOG.debug(f'Adding {topic} to tenant {tenant}')
        self.groups[tenant].add_topic(topic)

    def start_group(self, tenant):
        if not self.groups.get(tenant):
            LOG.debug(f'Adding new service for tenant: {tenant}')
            self.groups[tenant] = ESConsumerGroup(tenant)
            # Give time for the thread to start before completing the operation
            sleep(3)

    def stop_group(self, tenant):
        if self.groups.get(tenant):
            self.groups[tenant].stop()

    def stop(self, *args, **kwargs):
        self.stopped = True
        self.healthcheck.stop()
        for key in self.groups.keys():
            self.stop_group(key)


class AutoConfMaintainer(threading.Thread):

    def __init__(self, parent, autoconf=None):
        LOG.debug('Started Autoconf Maintainer')
        self.parent = parent
        self.autoconf = autoconf
        self.configured_topics = []
        self.consumer = KafkaConsumer(**KAFKA_CONFIG.copy())
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
                    if MT:
                        parts = topic.split('.')
                        tenant = parts[0]
                    else:
                        tenant = CONSUMER_CONFIG.get('default_tenant')
                    self.parent.notify_topic(tenant, topic)
                    self.configured_topics.append(topic)

            # Check running threads

            # TODO RE-enable liveness check

            # try:
            #     self.check_running_groups()
            # except Exception as err:
            #     LOG.error(f'Error watching running threads: {err}')
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
    # Consumer Group for a single tenant with a threaded consumer

    def __init__(self, tenant):
        LOG.debug(f'Consumer Group started for tenant: {tenant}')
        self.tenant = tenant
        self.consumer = ESConsumer(self.tenant)
        self.consumer.start()
        self.topics = []  # configuration for each topic

    def add_topic(self, topic):
        LOG.debug(f'Tenant {self.tenant} caught new topic {topic}')
        self.topics.append(topic)
        self.consumer.add_topic(topic)

    def is_alive(self):
        try:
            return self.consumer.is_alive()
        except KeyError as ker:
            LOG.error(f'Error getting liveness on {self.tenant}: {ker}')
            return False

    def monitor_threads(self):
        LOG.debug(f'Checking threads on group: {self.tenant}')
        if not self.is_alive():
            LOG.error(f'Tenant {self.tenant} died. Restarting.')
            # self.start_topic(self.consumer)

    def stop(self):
        self.consumer.stop()


class ESConsumer(threading.Thread):
    # A single consumer subscribed to all tenant topics
    # Runs as a daemon to avoid weird stops
    tenant_topic_re = re.compile(
        r'''topic:(?P<tenant>[^\.]*)\.(?P<name>.*)-partition:.*'''
    )
    kafka_group_template = '{tenant}.aether.es_consumer.group-{tenant}.v1'

    def __init__(self, tenant):  # , index, processor, has_group=True, doc_type=None):
        # has_group = False only used for testing
        LOG.debug(f'Initializing consumer for tenant: {tenant}')
        self.tenant = tenant
        self.group_name = ESConsumer.kafka_group_template.format(tenant=tenant)
        self.schemas = {}
        self.processors = {}
        self.indices = {}
        self.get_route = {}
        self.doc_types = {}
        self.topics = []
        self.autoconf = CONSUMER_CONFIG.get('autoconfig_settings', {})
        self.consumer_timeout = 8000  # MS
        self.consumer_max_records = 1000
        self.sleep_time = 10
        self.stopped = False
        self.consumer = None
        self.thread_id = 0
        super(ESConsumer, self).__init__()

    def add_topic(self, topic):
        LOG.debug(f'Adding {topic} to consumer thread: {self.tenant}')
        self.topics.append(topic)
        # subscribe to all topics
        try:
            self.consumer.subscribe(self.topics)
            LOG.debug(f'{self.tenant} subscribed on topics: {self.topics}')
        except Exception as ke:
            LOG.error('%s failed to subscibe to topic %s with error \n%s' %
                      (self.tenant, topic, ke))
            return False

    def name_from_topic(self, topic):
        return topic.lstrip(f'{self.tenant}.')

    def update_topic(self, topic, schema):
        # TODO Handle schema change
        pass

    def init_topic(self, topic, schema):
        LOG.debug(f'{self.tenant} is starting topic: {topic}')

        # TODO Handle schema change ^^

        # check if index exists
        #     make index
        index = index_handler.get_es_index_from_autoconfig(
            self.autoconf,
            self.tenant,
            self.name_from_topic(topic)
        )
        self.indices[topic] = index
        LOG.debug(f'{self.tenant}:{topic} | idx: {index}')
        #     make alias

        # update processor for type
        doc_type, instr = list(index['body']['mappings'].items())[0]
        # check if kibana index exists
        #     make kibana index

        self.doc_types[topic] = doc_type
        self.processors[topic] = ESItemProcessor(topic, instr)
        self.processors[topic].load_avro(schema)
        self.get_route[topic] = self.processors[topic].create_route()

    def connect(self):
        # have to get to force env lookups
        args = KAFKA_CONFIG.copy()
        args['client_id'] = self.group_name
        args['group_id'] = self.group_name
        try:
            LOG.debug(
                f'Kafka CONFIG [{self.thread_id}]'
                f'[{self.tenant}:{self.group_name}]')
            LOG.debug(json.dumps(args, indent=2))
            self.consumer = KafkaConsumer(**args)
            return True
        except Exception as ke:
            LOG.error(
                f'{self.tenant} failed to subscibe to kafka with error \n{ke}'
            )
            return False

    def run(self):
        self.thread_id = threading.get_ident()
        LOG.debug(f'Consumer [{self.thread_id}] running on {self.tenant}')
        while True:
            if self.connect():
                break
            elif self.stopped:
                return
            sleep(2)
        while not self.stopped:
            new_messages = self.consumer.poll_and_deserialize(
                timeout_ms=self.consumer_timeout,
                max_records=self.consumer_max_records)
            if not new_messages:
                LOG.info(
                    f'Kafka IDLE [{self.thread_id}]'
                    f'[{self.tenant}:{self.group_name}]')
                sleep(5)
                continue
            for parition_key, packages in new_messages.items():

                m = ESConsumer.tenant_topic_re.search(parition_key)
                tenant = m.group('tenant')
                name = m.group('name')
                topic = f'{tenant}.{name}'
                LOG.debug(f'read PK: {topic}')

                for package in packages:
                    schema = package.get('schema')
                    messages = package.get('messages')
                    LOG.debug('messages #%s' % len(messages))
                    if schema != self.schemas.get(topic):
                        LOG.info('Schema change on type %s' % topic)
                        LOG.debug('schema: %s' % schema)
                        self.init_topic(topic, schema)
                        self.schemas[topic] = schema
                    else:
                        LOG.debug('Schema unchanged.')
                    count = 0
                    processor = self.processors[topic]
                    index_name = self.indices[topic]['name']
                    doc_type = self.doc_types[topic]
                    route_getter = self.get_route[topic]

                    for x, msg in enumerate(messages):
                        doc = processor.process(msg)
                        count = x
                        LOG.debug(
                            f'Kafka READ [{self.thread_id}]'
                            f'[{topic}:{self.group_name}]'
                            f' -> {doc.get("id")}')
                        self.submit(
                            index_name,
                            doc_type,
                            doc,
                            topic,
                            route_getter
                        )
                    LOG.debug(
                        f'Kafka COMMIT [{self.thread_id}]'
                        f'[{topic}:{self.group_name}]')
                    self.consumer.commit_async(callback=self.report_commit)
                    LOG.info('processed %s %s docs in tenant %s' %
                             ((count + 1), name, self.tenant))

        LOG.info(f'Shutting down consumer {self.tenant}')
        self.consumer.close(autocommit=True)
        return

    def report_commit(self, offsets, response):
        LOG.info(f'Kafka OFFSET CMT {offsets} -> {response}')

    def submit(self, index_name, doc_type, doc, topic, route_getter):
        parent = doc.get('_parent', None)
        if parent:  # _parent field can only be in metadata apparently
            del doc['_parent']
        route = route_getter(doc)
        try:
            # route = self.get_route[topic](doc)
            es.create(
                index=index_name,
                id=doc.get('id'),
                routing=route,
                doc_type=doc_type,
                body=doc
            )
            LOG.debug(
                f'ES CREATE-OK [{self.thread_id}]'
                f'[{index_name}:{self.group_name}]'
                f' -> {doc.get("id")}')

        except (Exception, TransportError) as ese:
            LOG.info('Could not create doc because of error: %s\nAttempting update.' % ese)
            try:
                route = self.get_route[topic](doc)
                es.update(
                    index=index_name,
                    id=doc.get('id'),
                    routing=route,
                    doc_type=self.doc_type,
                    body=doc
                )
                LOG.debug(
                    f'ES UPDATE-OK [{self.thread_id}]'
                    f'[{index_name}:{self.group_name}]'
                    f' -> {doc.get("id")}')
            except TransportError:
                LOG.debug('conflict exists, ignoring document with id %s' %
                          doc.get('id', 'unknown'))

    def stop(self):
        LOG.info('%s caught stop signal' % (self.group_name))
        self.stopped = True


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

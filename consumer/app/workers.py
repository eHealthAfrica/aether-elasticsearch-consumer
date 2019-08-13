#!/usr/bin/env python

# Copyright (C) 2019 by eHealth Africa : http://www.eHealthAfrica.org
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
from time import sleep
import threading
from typing import Any, Mapping

from aet.consumer import KafkaConsumer
from elasticsearch.exceptions import TransportError

from . import config, connection_handler, index_handler
from .processor import ESItemProcessor
from .logger import get_logger
from .schema import Node

from .connection_handler import KibanaConnection

LOG = get_logger('WORKER')
CONSUMER_CONFIG = config.get_consumer_config()
KAFKA_CONFIG = config.get_kafka_config()

ES = connection_handler.ESConnectionManager(add_default=True)


class ESWorker(threading.Thread):
    # A single consumer subscribed to all tenant topics
    # Runs as a daemon to avoid weird stops
    tenant_topic_re = re.compile(
        r'''topic:(?P<tenant>[^\.]*)\.(?P<name>.*)-partition:.*'''
    )
    kafka_group_template = '{tenant}.aether.es_consumer.group-{tenant}.v1'

    def __init__(self, tenant):
        LOG.debug(f'Initializing consumer for tenant: {tenant}')
        self.tenant = tenant
        self.group_name = ESWorker.kafka_group_template.format(tenant=tenant)
        self.schemas = {}
        self.processors = {}
        self.es_instances = {}
        self.kibana_instances = {}
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
        super(ESWorker, self).__init__()

    def add_topic(self, topic):
        LOG.debug(f'Adding {topic} to consumer thread: {self.tenant}')
        self.topics.append(topic)
        # TODO align related ES / Kibana instances
        self.es_instances[topic] = [ES.get_connection()]
        self.kibana_instances[topic] = [ES.get_kibana()]
        self._update_topics()

    def add_topics(self, topics):
        LOG.debug(f'Adding {(topics,)} to consumer thread: {self.tenant}')
        self.topics.extend(topics)
        for topic in topics:
            self.es_instances[topic] = [ES.get_connection()]
            self.kibana_instances[topic] = [ES.get_kibana()]
        self._update_topics()

    def _update_topics(self):
        # subscribe to all topics (overrides existing list of subscriptions)
        try:
            self.consumer.subscribe(self.topics)
            LOG.debug(f'{self.tenant} subscribed on topics: {self.topics}')
            return True
        except Exception as ke:
            LOG.error('%s failed to subscibe to topic %s with error \n%s' %
                      (self.tenant, (self.topics,), ke))
            return False

    def name_from_topic(self, topic):
        return topic.lstrip(f'{self.tenant}.')

    def update_topic(self, topic, schema: Mapping[Any, Any]):
        LOG.debug(f'{self.tenant} is updating topic: {topic}')

        node: Node = Node(schema)
        es_index = index_handler.get_es_index_from_autoconfig(
            self.autoconf,
            name=self.name_from_topic(topic),
            tenant=self.tenant,
            schema=node
        )
        alias = index_handler.get_alias_from_namespace(
            self.tenant, node.namespace
        )
        # Try to add the indices / ES alias
        for es_instance in self.es_instances.get(topic, []):
            LOG.debug(f'registering ES index:\n{json.dumps(es_index, indent=2)}')
            updated = index_handler.register_es_index(
                es_instance,
                es_index,
                alias
            )
            if updated:
                LOG.debug(f'{self.tenant} updated schema for {topic}')
            # TODO, update connection get for multiple kibana case
            conn: KibanaConnection = self.kibana_instances[topic][0]

            old_schema = self.schemas.get(topic)
            updated_kibana = index_handler.kibana_handle_schema_change(
                self.tenant,
                alias,
                old_schema,
                schema,
                es_index,
                es_instance,
                conn
            )

            if updated_kibana:
                LOG.info(
                    f'Registered kibana index {alias} for {self.tenant}'
                )
            else:
                LOG.info(
                    f'Kibana index {alias} did not need update.'
                )

        self.indices[topic] = es_index
        LOG.debug(f'{self.tenant}:{topic} | idx: {es_index}')
        # update processor for type
        doc_type, instr = list(es_index['body']['mappings'].items())[0]
        self.doc_types[topic] = doc_type
        self.processors[topic] = ESItemProcessor(topic, instr)
        self.processors[topic].load_avro(schema)
        self.get_route[topic] = self.processors[topic].create_route()

    def connect(self):
        # have to get to force env lookups
        args = KAFKA_CONFIG.copy()
        args['client_id'] = self.group_name
        args['group_id'] = self.group_name + '_005'  # TODO KILL!
        args['enable_auto_commit'] = False
        try:
            LOG.debug(
                f'Kafka CONFIG[{self.tenant}:{self.group_name}]')
            LOG.debug(json.dumps(args, indent=2))
            self.consumer = KafkaConsumer(**args)
            return True
        except Exception as ke:
            LOG.error(
                f'{self.tenant} failed to subscibe to kafka with error \n{ke}'
            )
            return False

    def run(self):
        LOG.debug(f'Consumer running on {self.tenant}')
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
                    f'Kafka IDLE [{self.tenant}:{self.topics}]')
                sleep(5)
                continue
            for parition_key, packages in new_messages.items():

                m = ESWorker.tenant_topic_re.search(parition_key)
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
                        self.update_topic(topic, schema)
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
                            f'Kafka READ [{topic}:{self.group_name}]'
                            f' -> {doc.get("id")}')
                        for es_instance in self.es_instances[topic]:
                            self.submit(
                                index_name,
                                doc_type,
                                doc,
                                topic,
                                route_getter,
                                es_instance
                            )
                    LOG.debug(
                        f'Kafka COMMIT [{topic}:{self.group_name}]')
                    self.consumer.commit_async(callback=self.report_commit)
                    LOG.info('processed %s %s docs in tenant %s' %
                             ((count + 1), name, self.tenant))

        LOG.info(f'Shutting down consumer {self.tenant}')
        self.consumer.close(autocommit=True)
        return

    def report_commit(self, offsets, response):
        LOG.info(f'Kafka OFFSET CMT {offsets} -> {response}')

    def submit(self, index_name, doc_type, doc, topic, route_getter, es):
        parent = doc.get('_parent', None)
        if parent:  # _parent field can only be in metadata apparently
            del doc['_parent']
        route = route_getter(doc)
        try:
            es.create(
                index=index_name,
                id=doc.get('id'),
                routing=route,
                doc_type=doc_type,
                body=doc
            )
            LOG.debug(
                f'ES CREATE-OK [{index_name}:{self.group_name}]'
                f' -> {doc.get("id")}')

        except (Exception, TransportError) as ese:
            LOG.info('Could not create doc because of error: %s\nAttempting update.' % ese)
            try:
                route = self.get_route[topic](doc)
                es.update(
                    index=index_name,
                    id=doc.get('id'),
                    routing=route,
                    doc_type=doc_type,
                    body=doc
                )
                LOG.debug(
                    f'ES UPDATE-OK [{index_name}:{self.group_name}]'
                    f' -> {doc.get("id")}')
            except TransportError:
                LOG.debug('conflict exists, ignoring document with id %s' %
                          doc.get('id', 'unknown'))

    def stop(self):
        LOG.info('%s caught stop signal' % (self.group_name))
        self.stopped = True

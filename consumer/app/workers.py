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
from time import sleep
import threading
from typing import Any, Mapping

from aet.kafka import KafkaConsumer
from elasticsearch.exceptions import TransportError

from . import config, index_handler
from .processor import ESItemProcessor
from .logger import get_logger
from .schema import Node

from .connection_handler import KibanaConnection, ESConnectionManager

LOG = get_logger('WORKER')
CONSUMER_CONFIG = config.get_consumer_config()
KAFKA_CONFIG = config.get_kafka_config()


class ESWorker(threading.Thread):
    # A single consumer subscribed to all tenant topics
    # Runs as a daemon to avoid weird stops

    kafka_group_template = '{tenant}.aether.es_consumer.group-{tenant}.v1'

    def __init__(self, tenant, conn: ESConnectionManager):
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
        self.conn = conn
        super(ESWorker, self).__init__()

    def add_topic(self, topic):
        LOG.debug(f'Adding {topic} to consumer thread: {self.tenant}')
        self.topics.append(topic)
        # TODO align related ES / Kibana instances
        self.es_instances[topic] = [self.conn.get_connection()]
        self.kibana_instances[topic] = [self.conn.get_kibana()]
        self._update_topics()

    def add_topics(self, topics):
        LOG.debug(f'Adding {(topics,)} to consumer thread: {self.tenant}')
        self.topics.extend(topics)
        for topic in topics:
            self.es_instances[topic] = [self.conn.get_connection()]
            self.kibana_instances[topic] = [self.conn.get_kibana()]
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
        args['client.id'] = self.group_name
        args['group.id'] = self.group_name + '_010'
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

    def test_es_connection(self):
        unique_es = {
            es.instance_id: es
            for topic, eses in self.es_instances.items()
            for es in eses
        }
        for _id, es in unique_es.items():
            if not ESConnectionManager.connection_is_live(es):
                raise ConnectionError(
                    f'ES instance {_id} could not be contacted')
            else:
                LOG.debug(f'ES {_id} ready')
        unique_kibana = {
            kb.instance_id: kb
            for topic, kbs in self.kibana_instances.items()
            for kb in kbs
        }
        for _id, kb in unique_kibana.items():
            if not kb.test(self.tenant):
                raise ConnectionError(
                    f'Kibana instance {_id} could not be contacted')
            else:
                LOG.debug(f'Kibana {_id} ready')

    def run(self):
        LOG.debug(f'Consumer running on {self.tenant}')
        while True:
            if self.connect():
                break
            elif self.stopped:
                return
            sleep(2)
        while not self.stopped:
            try:
                # don't fetch messages if we can't post them
                self.test_es_connection()
                new_messages = self.consumer.poll_and_deserialize(
                    timeout=1,
                    num_messages=self.consumer_max_records)
            except ConnectionError as cer:
                LOG.debug(f'ES or Kibana not ready: {cer}')
                new_messages = []
            if not new_messages:
                LOG.info(
                    f'Kafka IDLE [{self.tenant}:{self.topics}]')
                sleep(5)
                continue
            count = 0
            for msg in new_messages:
                topic = msg.topic
                LOG.debug(f'read PK: {topic}')
                schema = msg.schema
                if schema != self.schemas.get(topic):
                    LOG.info('Schema change on type %s' % topic)
                    LOG.debug('schema: %s' % schema)
                    self.update_topic(topic, schema)
                    self.schemas[topic] = schema
                else:
                    LOG.debug('Schema unchanged.')
                processor = self.processors[topic]
                index_name = self.indices[topic]['name']
                doc_type = self.doc_types[topic]
                route_getter = self.get_route[topic]
                doc = processor.process(msg.value)
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
                count += 1
            LOG.info('processed %s %s docs in tenant %s' %
                     ((count), topic, self.tenant))

        LOG.info(f'Shutting down consumer {self.tenant}')
        self.consumer.close()
        return

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

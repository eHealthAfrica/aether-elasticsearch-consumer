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

import fnmatch
import json
import logging
import requests
from time import sleep
from typing import (
    Any,
    Callable,
    List,
    Mapping
)
from urllib3.exceptions import NewConnectionError
from uuid import uuid4

from confluent_kafka import KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from elasticsearch.exceptions import AuthenticationException as ESAuthenticationException
from elasticsearch.exceptions import TransportError as ESTransportError

# Consumer SDK
from aet.exceptions import ConsumerHttpException
from aet.job import BaseJob
from aet.kafka import KafkaConsumer
# from aet.logger import callback_logger, get_logger
from aet.resource import BaseResource, Draft7Validator, lock

# Aether python lib
from aether.python.avro.schema import Node

from .. import index_handler
from ..config import get_consumer_config, get_kafka_config
from ..logger import callback_logger, get_logger
from ..fixtures import schemas
from ..processor import ESItemProcessor


LOG = get_logger('artifacts')
CONSUMER_CONFIG = get_consumer_config()
KAFKA_CONFIG = get_kafka_config()

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.ERROR)


class ESInstance(BaseResource):
    schema = schemas.ES_INSTANCE
    jobs_path = '$.elasticsearch'
    name = 'elasticsearch'
    public_actions = BaseResource.public_actions + [
        'test_connection'
    ]

    session: Elasticsearch = None

    @lock
    def get_session(self):
        if self.session:
            return self.session
        url = self.definition.url
        conn_info = {
            'sniff_on_start': False
        }
        try:
            conn_info['http_auth'] = [
                self.definition.user,
                self.definition.password
            ]
        except AttributeError:
            pass
        try:
            self.session = Elasticsearch(url, **conn_info)
        except Exception as err:
            raise err
        # add an _id so we can check the instance
        setattr(self.session, 'instance_id', str(uuid4()))
        return self.session

    def test_connection(self, *args, **kwargs):
        es = self.get_session()
        try:
            es_info = es.info()
            return es_info
        except (
            NewConnectionError,
            ConnectionRefusedError,
            ESConnectionError,
        ) as nce:
            raise ConsumerHttpException(nce, 500)
        except ESAuthenticationException as esa:
            raise ConsumerHttpException(str(esa.error), esa.status_code)
        except Exception as err:
            raise ConsumerHttpException(err, 500)


class LocalESInstance(ESInstance):
    schema = schemas.LOCAL_ES_INSTANCE
    jobs_path = '$.local_elasticsearch'
    name = 'local_elasticsearch'
    public_actions = ESInstance.public_actions

    @classmethod
    def _validate(cls, definition) -> bool:
        # subclassing a Resource a second time breaks some of the class methods
        if cls.validator == LocalESInstance.validator:
            cls.validator = Draft7Validator(json.loads(cls.schema))
        return super(LocalESInstance, cls)._validate(definition)

    @lock
    def get_session(self):
        if not self.definition.get('url'):
            self.definition['url'] = CONSUMER_CONFIG.get('elasticsearch_url')
            self.definition['user'] = CONSUMER_CONFIG.get('elasticsearch_user')
            self.definition['password'] = CONSUMER_CONFIG.get('elasticsearch_password')
        if self.session:
            return self.session
        url = self.definition.url
        conn_info = {
            'http_auth': [
                self.definition.user,
                self.definition.password
            ],
            'sniff_on_start': False
        }
        self.session = Elasticsearch(url, **conn_info)
        # add an _id so we can check the instance
        setattr(self.session, 'instance_id', str(uuid4()))
        return self.session


class KibanaInstance(BaseResource):
    schema = schemas.KIBANA_INSTANCE
    jobs_path = '$.kibana'
    name = 'kibana'
    public_actions = BaseResource.public_actions + [
        'test_connection'
    ]

    session: requests.Session = None

    def get_session(self):
        if self.session:
            return self.session
        self.session = requests.Session()
        try:
            self.session.auth = (
                self.definition.user,
                self.definition.password
            )
        except AttributeError:
            pass  # may not need creds
        self.session.headers.update({'kbn-xsrf': 'f'})  # required header
        return self.session

    @lock
    def request(self, method, url, **kwargs):
        try:
            session = self.get_session()
        except Exception as err:
            raise ConsumerHttpException(str(err), 500)
        full_url = f'{self.definition.url}{url}'
        return session.request(method, full_url, **kwargs)

    # public
    def test_connection(self, *args, **kwargs) -> bool:
        '''
        Test the connection to this Kibana Instance
        If the request fails to connect, the Error is returned to the user.
        If the request completes, a Boolean for success is returned.
        '''
        try:
            res = self.request('head', '')
        except requests.exceptions.ConnectionError as her:
            raise ConsumerHttpException(str(her), 500)
        except Exception as err:
            LOG.debug(f'Error testing kibana connection {err}, {type(err)}')
            raise ConsumerHttpException(err, 500)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as her:
            LOG.debug(f'Error testing kibana connection {her}')
            raise ConsumerHttpException(her, her.response.status_code)
        return True


class LocalKibanaInstance(KibanaInstance):
    schema = schemas.LOCAL_KIBANA_INSTANCE
    jobs_path = '$.local_kibana'
    name = 'local_kibana'
    # public_actions = KibanaInstance.public_actions

    @classmethod
    def _validate(cls, definition) -> bool:
        # subclassing a Resource a second time breaks some of the class methods
        if cls.validator == KibanaInstance.validator:
            cls.validator = Draft7Validator(json.loads(cls.schema))
        return super(LocalKibanaInstance, cls)._validate(definition)

    def get_session(self):
        if not self.definition.get('url'):
            self.definition['url'] = CONSUMER_CONFIG.get('kibana_url')
        if self.session:
            return self.session
        self.session = requests.Session()
        headers = {
            'x-forwarded-for': '255.0.0.1',
            'x-oauth-preferred_username': 'aether-consumer',
            'x-oauth-realm': self.tenant,
            'kbn-xsrf': 'f'
        }
        self.session.headers.update(headers)
        return self.session


class Subscription(BaseResource):
    schema = schemas.SUBSCRIPTION
    jobs_path = '$.subscriptions'
    name = 'subscription'

    def _handles_topic(self, topic, tenant):
        topic_str = self.definition.topic_pattern
        # remove tenant information
        topic.lstrip(f'{tenant}.')
        return fnmatch.fnmatch(topic, topic_str)


class ESJob(BaseJob):
    name = 'job'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources = [ESInstance, LocalESInstance, KibanaInstance, LocalKibanaInstance, Subscription]
    schema = schemas.ES_JOB

    public_actions = BaseResource.public_actions + [
        'get_logs',
        'list_topics',
        'list_subscribed_topics'
    ]
    # publicly available list of topics
    subscribed_topics: dict
    log_stack: list
    log: Callable  # each job instance has it's own log object to keep log_stacks -> user reportable

    consumer: KafkaConsumer = None
    # processing artifacts
    _indices: dict
    _schemas: dict
    _processors: dict
    _doc_types: dict
    _routes: dict
    _kibana: KibanaInstance
    _elasticsearch: ESInstance
    _subscriptions: List[Subscription]

    def _handle_new_topics(self, subs):
        old_subs = list(sorted(set(self.subscribed_topics.values())))
        for sub in subs:
            pattern = sub.definition.topic_pattern
            # only allow regex on the end of patterns
            if pattern.endswith('*'):
                self.subscribed_topics[sub.id] = f'^{self.tenant}.{pattern}'
            else:
                self.subscribed_topics[sub.id] = f'{self.tenant}.{pattern}'
        new_subs = list(sorted(set(self.subscribed_topics.values())))
        if old_subs != new_subs:
            self.log.debug(f'{self.tenant} subscribed on topics: {new_subs}')
            self.consumer.subscribe(new_subs)

    def _job_elasticsearch(self, config=None) -> ESInstance:
        if config:
            es = self.get_resources('local_elasticsearch', config) + \
                self.get_resources('elasticsearch', config)
            if not es:
                raise ConsumerHttpException('No ES associated with Job', 400)
            self._elasticsearch = es[0]
        return self._elasticsearch

    def _job_kibana(self, config=None) -> KibanaInstance:
        if config:
            kibana = self.get_resources('local_kibana', config) + \
                self.get_resources('kibana', config)
            if not kibana:
                raise ConsumerHttpException('No Kibana associated with Job', 400)
            self._kibana = kibana[0]
        return self._kibana

    def _job_subscriptions(self, config):
        if config:
            subs = self.get_resources('subscriptions', config)
            if not subs:
                raise ConsumerHttpException('No Subscriptions associated with Job', 400)
            self._subscriptions = subs
        return self._subscriptions

    def _test_connections(self, config):
        self._job_subscriptions(config)
        self._job_elasticsearch(config).test_connection()  # raises CHE
        self._job_kibana(config).test_connection()  # raises CHE
        return True

    def _get_messages(self, config):
        try:
            self.log.debug(f'{self._id} checking configurations...')
            self._test_connections(config)
            subs = self._job_subscriptions()
            self._handle_new_topics(subs)
            self.log.debug(f'Job {self._id} getting messages')
            assignment = self.consumer.assignment()
            self.log.debug(f'assigned to {assignment}')
            return self.consumer.poll_and_deserialize(
                timeout=5,
                num_messages=1)  # max
        except ConsumerHttpException as cer:
            # don't fetch messages if we can't post them
            self.log.debug(f'Job not ready: {cer}')
            sleep(.25)
            return []
        except Exception as err:
            import traceback
            traceback_str = ''.join(traceback.format_tb(err.__traceback__))
            self.log.critical(f'unhandled error: {str(err)} | {traceback_str}')
            raise err
            sleep(.25)
            return []

    def _handle_messages(self, config, messages):
        count = 0
        for msg in messages:
            topic = msg.topic
            self.log.debug(f'read PK: {topic}')
            schema = msg.schema
            if schema != self._schemas.get(topic):
                self.log.info('Schema change on type %s' % topic)
                self.log.debug('schema: %s' % schema)
                self._update_topic(topic, schema)
                self._schemas[topic] = schema
            else:
                self.log.debug('Schema unchanged.')
            processor = self._processors[topic]
            index_name = self._indices[topic]['name']
            doc_type = self._doc_types[topic]
            route_getter = self._routes[topic]
            doc = processor.process(msg.value)
            self.log.debug(
                f'Kafka READ [{topic}:{self.group_name}]'
                f' -> {doc.get("id")}')
            self.submit(
                index_name,
                doc_type,
                doc,
                topic,
                route_getter,
            )
            self.log.debug(
                f'Kafka COMMIT [{topic}:{self.group_name}]')
            count += 1
        self.log.info(f'processed {count} {topic} docs in tenant {self.tenant}')

    def _update_topic(self, topic, schema: Mapping[Any, Any]):
        self.log.debug(f'{self.tenant} is updating topic: {topic}')

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
        es_instance = self._job_elasticsearch().get_session()
        self.log.debug(f'registering ES index:\n{json.dumps(es_index, indent=2)}')
        updated = index_handler.register_es_index(
            es_instance,
            es_index,
            alias
        )
        if updated:
            self.log.debug(f'{self.tenant} updated schema for {topic}')
        conn: KibanaInstance = self._job_kibana()

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
            self.log.info(
                f'Registered kibana index {alias} for {self.tenant}'
            )
        else:
            self.log.info(
                f'Kibana index {alias} did not need update.'
            )

        self._indices[topic] = es_index
        self.log.debug(f'{self.tenant}:{topic} | idx: {es_index}')
        # update processor for type
        doc_type, instr = list(es_index['body']['mappings'].items())[0]
        self._doc_types[topic] = doc_type
        self._processors[topic] = ESItemProcessor(topic, instr)
        self._processors[topic].load_avro(schema)
        self. _routes[topic] = self.processors[topic].create_route()

    def submit(self, index_name, doc_type, doc, topic, route_getter):
        es = self._job_elasticsearch().get_session()
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
            self.log.debug(
                f'ES CREATE-OK [{index_name}:{self.group_name}]'
                f' -> {doc.get("id")}')

        except (Exception, ESTransportError) as ese:
            self.log.info('Could not create doc because of error: %s\nAttempting update.' % ese)
            try:
                route = self. _routes[topic](doc)
                es.update(
                    index=index_name,
                    id=doc.get('id'),
                    routing=route,
                    doc_type=doc_type,
                    body=doc
                )
                self.log.debug(
                    f'ES UPDATE-OK [{index_name}:{self.group_name}]'
                    f' -> {doc.get("id")}')
            except ESTransportError:
                self.log.debug(f'''conflict!, ignoring doc with id {doc.get('id', 'unknown')}''')

    def _setup(self):
        self.subscribed_topics = {}
        self._indices = {}
        self._schemas = {}
        self._processors = {}
        self._doc_types = {}
        self._routes = {}
        self._subscriptions = []
        self.log_stack = []
        self.log = callback_logger('JOB', self.log_stack, 100)
        args = {k.lower(): v for k, v in KAFKA_CONFIG.copy().items()}
        self.consumer = KafkaConsumer(**args)

    # public
    def list_topics(self, *args, **kwargs):
        timeout = 5
        try:
            md = self.consumer.list_topics(timeout=timeout)
        except (KafkaException) as ker:
            raise ConsumerHttpException(str(ker) + f'@timeout: {timeout}', 500)
        topics = [
            str(t).split(f'{self.tenant}.')[1] for t in iter(md.topics.values())
            if str(t).startswith(self.tenant)
        ]
        return topics

    # public
    def list_subscribed_topics(self, *arg, **kwargs):
        return list(self.subscribed_topics.values())

    # public
    def get_logs(self, *arg, **kwargs):
        return self.log_stack[:]

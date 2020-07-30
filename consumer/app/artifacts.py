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
import traceback
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
from aet.job import BaseJob, JobStatus
from aet.kafka import KafkaConsumer, FilterConfig, MaskConfig
from aet.logger import get_logger
from aet.resource import BaseResource, Draft7Validator, lock

# Aether python lib
from aether.python.avro.schema import Node

from app import index_handler
from app.config import get_consumer_config, get_kafka_config
from app.fixtures import schemas
from app.processor import ESItemProcessor


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
    _masked_fields: List[str] = ['$.password']

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
    _masked_fields: List[str] = []

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
    _masked_fields: List[str] = ['$.password']

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
        res = session.request(method, full_url, **kwargs)
        try:
            res.raise_for_status()
        except Exception:
            raise ConsumerHttpException(str(res.content), res.status_code)
        return res

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
    _masked_fields: List[str] = []

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
            'x-oauth-preferred_username': 'user',
            'x-oauth-realm': self.tenant,
            'kbn-xsrf': 'f'
        }
        self.session.headers.update(headers)
        return self.session


class Subscription(BaseResource):
    schema = schemas.SUBSCRIPTION
    jobs_path = '$.subscription'
    name = 'subscription'

    def _handles_topic(self, topic, tenant):
        topic_str = self.definition.topic_pattern
        # remove tenant information
        no_tenant = topic[:].replace(f'{tenant}.', '', 1)
        return fnmatch.fnmatch(no_tenant, topic_str)


class ESJob(BaseJob):
    name = 'job'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources = [ESInstance, LocalESInstance, KibanaInstance, LocalKibanaInstance, Subscription]
    schema = schemas.ES_JOB

    public_actions = BaseJob.public_actions + [
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
    _previous_topics: list
    _kibana: KibanaInstance
    _elasticsearch: ESInstance
    _subscriptions: List[Subscription]

    def _setup(self):
        self.subscribed_topics = {}
        self._indices = {}
        self._schemas = {}
        self._processors = {}
        self._doc_types = {}
        self._routes = {}
        self._subscriptions = []
        self._previous_topics = []
        self.group_name = f'{self.tenant}.esconsumer.{self._id}'
        self.sleep_delay: float = 0.5
        self.report_interval: int = 100
        args = {k.lower(): v for k, v in KAFKA_CONFIG.copy().items()}
        args['group.id'] = self.group_name
        LOG.debug(args)
        self.consumer = KafkaConsumer(**args)

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

    def _job_subscriptions(self, config=None) -> List[Subscription]:
        if config:
            subs = self.get_resources('subscription', config)
            if not subs:
                raise ConsumerHttpException('No Subscriptions associated with Job', 400)
            self._subscriptions = subs
        return self._subscriptions

    def _job_subscription_for_topic(self, topic):
        return next(iter(
            sorted([
                i for i in self._job_subscriptions()
                if i._handles_topic(topic, self.tenant)
            ])),
            None)

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
            self._handle_new_subscriptions(subs)
            self.log.debug(f'Job {self._id} getting messages')
            return self.consumer.poll_and_deserialize(
                timeout=5,
                num_messages=1)  # max
        except ConsumerHttpException as cer:
            # don't fetch messages if we can't post them
            self.log.debug(f'Job not ready: {cer}')
            self.status = JobStatus.RECONFIGURE
            sleep(self.sleep_delay * 10)
            return []
        except Exception as err:
            traceback_str = ''.join(traceback.format_tb(err.__traceback__))
            self.log.critical(f'unhandled error: {str(err)} | {traceback_str}')
            raise err
            sleep(self.sleep_delay)
            return []

    def _handle_new_subscriptions(self, subs):
        old_subs = list(sorted(set(self.subscribed_topics.values())))
        for sub in subs:
            pattern = sub.definition.topic_pattern
            # only allow regex on the end of patterns
            if pattern.endswith('*'):
                self.subscribed_topics[sub.id] = f'^{self.tenant}.{pattern}'
            else:
                self.subscribed_topics[sub.id] = f'{self.tenant}.{pattern}'
        new_subs = list(sorted(set(self.subscribed_topics.values())))
        _diff = list(set(old_subs).symmetric_difference(set(new_subs)))
        if _diff:
            self.log.info(f'{self.tenant} added subs to topics: {_diff}')
            self.consumer.subscribe(new_subs, on_assign=self._on_assign)

    def _handle_messages(self, config, messages):
        self.log.debug(f'{self.group_name} | reading {len(messages)} messages')
        count = 0
        for msg in messages:
            topic = msg.topic
            schema = msg.schema
            if schema != self._schemas.get(topic):
                self.log.info(f'{self._id} Schema change on {topic}')
                self._update_topic(topic, schema)
                self._schemas[topic] = schema
            else:
                self.log.debug('Schema unchanged.')
            processor = self._processors[topic]
            index_name = self._indices[topic]['name']
            doc_type = self._doc_types[topic]
            route_getter = self._routes[topic]
            doc = processor.process(msg.value)
            self.submit(
                index_name,
                doc_type,
                doc,
                topic,
                route_getter,
            )
            count += 1
        self.log.info(f'processed {count} {topic} docs in tenant {self.tenant}')

    # called when a subscription causes a new assignment to be given to the consumer
    def _on_assign(self, *args, **kwargs):
        assignment = args[1]
        for _part in assignment:
            if _part.topic not in self._previous_topics:
                self.log.info(f'New topic to configure: {_part.topic}')
                self._apply_consumer_filters(_part.topic)
                self._previous_topics.append(_part.topic)

    def _apply_consumer_filters(self, topic):
        self.log.debug(f'{self._id} applying filter for new topic {topic}')
        subscription = self._job_subscription_for_topic(topic)
        if not subscription:
            self.log.error(f'Could not find subscription for topic {topic}')
            return
        try:
            opts = subscription.definition.topic_options
            _flt = opts.get('filter_required', False)
            if _flt:
                _filter_options = {
                    'check_condition_path': opts.get('filter_field_path', ''),
                    'pass_conditions': opts.get('filter_pass_values', []),
                    'requires_approval': _flt
                }
                self.log.info(_filter_options)
                self.consumer.set_topic_filter_config(
                    topic,
                    FilterConfig(**_filter_options)
                )
            mask_annotation = opts.get('masking_annotation', None)
            if mask_annotation:
                _mask_options = {
                    'mask_query': mask_annotation,
                    'mask_levels': opts.get('masking_levels', []),
                    'emit_level': opts.get('masking_emit_level')
                }
                self.log.info(_mask_options)
                self.consumer.set_topic_mask_config(
                    topic,
                    MaskConfig(**_mask_options)
                )
            self.log.info(f'Filters applied for topic {topic}')
        except AttributeError as aer:
            self.log.error(f'No topic options for {subscription.id}| {aer}')

    def _name_from_topic(self, topic):
        return topic[:].replace(f'{self.tenant}.', '', 1)

    def _update_topic(self, topic, schema: Mapping[Any, Any]):
        self.log.debug(f'{self.tenant} is updating topic: {topic}')
        subscription = self._job_subscription_for_topic(topic)
        node: Node = Node(schema)
        self.log.debug('getting index')
        es_index = index_handler.get_es_index_from_subscription(
            subscription.definition.get('es_options'),
            name=self._name_from_topic(topic),
            tenant=self.tenant.lower(),
            schema=node
        )
        self.log.debug(f'index {es_index}')
        alias_request = subscription.definition.get('es_options', {}).get('alias_name')
        if alias_request:
            alias = f'{alias_request}'.lower()
        else:
            alias = index_handler.get_alias_from_namespace(node.namespace)
        # Try to add the indices / ES alias
        es_instance = self._job_elasticsearch().get_session()
        if index_handler.es_index_changed(es_instance, es_index, self.tenant):
            self.log.debug(f'{self.tenant} updated schema for {topic}')
            self.log.debug(f'registering ES index:\n{json.dumps(es_index, indent=2)}')
            index_handler.update_es_index(
                es_instance,
                es_index,
                self.tenant,
                alias
            )
        conn: KibanaInstance = self._job_kibana()

        old_schema = self._schemas.get(topic)
        updated_kibana = index_handler.kibana_handle_schema_change(
            self.tenant.lower(),
            alias,
            old_schema,
            schema,
            subscription.definition,
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
        self._processors[topic] = ESItemProcessor(topic, instr, node)
        # self._processors[topic].load_avro(schema)
        self._routes[topic] = self._processors[topic].create_route()

    def submit(self, index_name, doc_type, doc, topic, route_getter):
        es = self._job_elasticsearch().get_session()
        parent = doc.get('_parent', None)
        if parent:  # _parent field can only be in metadata apparently
            del doc['_parent']
        route = route_getter(doc)
        _id = doc.get('id')
        try:
            es.create(
                index=index_name,
                id=_id,
                routing=route,
                doc_type=doc_type,
                body=doc
            )
            self.log.debug(
                f'ES CREATE-OK [{index_name}:{self.group_name}]'
                f' -> {_id}')

        except (Exception, ESTransportError) as ese:
            self.log.debug('Could not create doc because of error: %s\nAttempting update.' % ese)
            try:
                route = self._routes[topic](doc)
                es.update(
                    index=index_name,
                    id=_id,
                    routing=route,
                    doc_type=doc_type,
                    body={'doc': doc}
                )
                self.log.debug(
                    f'ES UPDATE-OK [{index_name}:{self.group_name}]'
                    f' -> {_id}')
            except ESTransportError as ese2:
                self.log.info(
                    f'''conflict!, ignoring doc with id {_id}'''
                    f'{ese2}'
                )

    # public
    def list_topics(self, *args, **kwargs):
        '''
        Get a list of topics to which the job can subscribe.
        You can also use a wildcard at the end of names like:
        Name* which would capture both Name1 && Name2, etc
        '''
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
        '''
        A List of topics currently subscribed to by this job
        '''
        return list(self.subscribed_topics.values())

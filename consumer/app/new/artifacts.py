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
import logging
import requests
from time import sleep
from urllib3.exceptions import NewConnectionError
from uuid import uuid4

from confluent_kafka import KafkaException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError

from aet.exceptions import ConsumerHttpException
from aet.job import BaseJob
from aet.kafka import KafkaConsumer
from aet.logger import get_logger
from aet.resource import BaseResource, Draft7Validator, lock


from ..config import get_consumer_config, get_kafka_config
from ..fixtures import schemas


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

    def test_connection(self, *args, **kwargs):
        es = self.get_session()
        try:
            es_info = es.info()
            return es_info
        except (
            NewConnectionError,
            ConnectionRefusedError,
            ESConnectionError
        ) as nce:
            raise ConsumerHttpException(nce, 500)


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
        self.session.auth = (
            self.definition.user,
            self.definition.password
        )
        self.session.headers.update({'kbn-xsrf': 'f'})  # required header
        return self.session

    @lock
    def request(self, method, url, **kwargs):
        session = self.get_session()
        full_url = f'{self.definition.url}{url}'
        LOG.debug([method, full_url, kwargs])
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
        except Exception as err:
            raise ConsumerHttpException(err, 404)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as her:
            LOG.debug(f'Error testing kibana connection [{self.tenant}:{self.name}] : {her}')
            raise ConsumerHttpException(her, 500)
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
            'x-oauth-realm': self.realm,
            'kbn-xsrf': 'f'
        }
        self.session.headers.update(headers)
        return self.session


class Subscription(BaseResource):
    schema = schemas.SUBSCRIPTION
    jobs_path = '$.subscriptions'
    name = 'subscription'


class ESJob(BaseJob):
    name = 'job'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources = [ESInstance, LocalESInstance, KibanaInstance, LocalKibanaInstance, Subscription]
    schema = schemas.ES_JOB

    public_actions = BaseResource.public_actions + [
        'list_topics',
        'subscribed_topics'
    ]
    subscribed_topics: dict
    consumer: KafkaConsumer = None

    def _handle_new_topics(self, subs):
        old_subs = list(sorted(self.subscribed_topics.values()))
        for sub in subs:
            self.subscribed_topics[sub.id] = sub.definition.topic_pattern
        new_subs = list(sorted(self.subscribed_topics.values()))
        if old_subs != new_subs:
            LOG.debug(f'{self.tenant} subscribed on topics: {new_subs}')
            self.consumer.subscribe(new_subs)

    def _test_connections(self, config):
        es = self.get_resources('local_elasticsearch', config) + \
            self.get_resources('elasticsearch', config)
        if not es:
            raise ConsumerHttpException('No ES associated with Job', 400)
        es[0].test_connection()  # raises CHE
        kibana = self.get_resources('local_kibana', config) + \
            self.get_resources('kibana', config)
        if not kibana:
            raise ConsumerHttpException('No Kibana associated with Job', 400)
        kibana[0].test_connection()  # raises CHE
        return True

    def _get_messages(self, config):
        LOG.debug(f'Job {self._id} getting messages')
        subs = self.get_resources('subscription', config)
        if not subs:
            LOG.debug(f'Job {self._id} has no subscriptions...')
            sleep(.25)
            return []
        self._handle_new_topics(subs)
        try:
            self._test_connections(config)
            return self.consumer.poll_and_deserialize(
                timeout=1,
                num_messages=1000)  # max
        except ConsumerHttpException as cer:
            # don't fetch messages if we can't post them
            LOG.debug(f'ES or Kibana not ready: {cer}')
            sleep(.25)
            return []
        except Exception as err:
            LOG.debug(f'unhandled error: {err}')
            raise err
            sleep(.25)
            return []

    def _handle_messages(self, config):
        LOG.debug('handling messages')

    def _setup(self):
        self.subscribed_topics = {}
        args = {k.lower(): v for k, v in KAFKA_CONFIG.copy().items()}
        LOG.debug(json.dumps(args, indent=2))
        self.consumer = KafkaConsumer(**args)

    # public
    def list_topics(self, *args, **kwargs):
        timeout = 5
        try:
            md = self.consumer.list_topics(timeout=timeout)
        except (KafkaException) as ker:
            raise ConsumerHttpException(str(ker) + f'@timeout: {timeout}', 500)
        topics = [
            str(t) for t in iter(md.topics.values())
            if str(t).startswith(self.tenant)
        ]
        return topics

    # public
    def subscribed_topics(self, *arg, **kwargs):
        return self.subscribed_topics

    def broker_info(self, md):
        try:
            res = {'brokers': [], 'topics': []}
            for b in iter(md.brokers.values()):
                if b.id == md.controller_id:
                    res['brokers'].append('{}  (controller)'.format(b))
                else:
                    res['brokers'].append('{}'.format(b))
            for t in iter(md.topics.values()):
                t_str = []
                if t.error is not None:
                    errstr = ': {}'.format(t.error)
                else:
                    errstr = ''

                t_str.append('{} with {} partition(s){}'.format(t, len(t.partitions), errstr))

                for p in iter(t.partitions.values()):
                    if p.error is not None:
                        errstr = ': {}'.format(p.error)
                    else:
                        errstr = ''

                    t_str.append('partition {} leader: {}, replicas: {}, isrs: {}'.format(
                        p.id, p.leader, p.replicas, p.isrs, errstr))
                res['topics'].append(t_str)
            return res
        except Exception as err:
            return {'error': f'{err}'}

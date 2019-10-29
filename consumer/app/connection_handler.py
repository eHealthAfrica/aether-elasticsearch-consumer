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

# from copy import copy
from dataclasses import dataclass
import json
from requests.exceptions import HTTPError
import sys
from typing import Any, List, Mapping
from urllib3.exceptions import NewConnectionError
from uuid import uuid4

# from aet.consumer import KafkaConsumer
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError
import requests

from . import config, utils
from .logger import get_logger

# Feature flag for future non-default ES instances
# JOBS_ENABLED = False

LOG = get_logger('CONN')
SUPPORTED_ES_SERIES = [6, 7]


CONSUMER_CONFIG = config.get_consumer_config()
KAFKA_CONFIG = config.get_kafka_config()
CONN_RETRY = int(CONSUMER_CONFIG.get('startup_connection_retry'))


'''
    Kibana instances must be registered with an ES instance.
    As such, KibanaConnectionManager is used through the ES Manager API
'''
@dataclass
class KibanaConfig:
    '''Kibana configuration'''
    kibana_url: str
    kibana_user: str = None
    kibana_password: str = None
    kibana_headers: Mapping[str, Any] = None
    kibana_header_template: str = None


class KibanaConnection:
    '''Kibana Connection Helper Class'''
    def __init__(self, config: KibanaConfig):
        self.config = config
        self.base_url = self.config.kibana_url

    def _make_session(self):
        session = requests.Session()
        if self.config.kibana_user and self.config.kibana_password:
            session.auth = (
                self.config.kibana_user, self.config.kibana_password
            )
        return session

    def _get_headers(self, tenant=None):
        if self.config.kibana_header_template:
            return json.loads(
                (self.config.kibana_header_template % tenant)
            )
        else:
            return {
                'kbn-xsrf': 'f'
            }

        elif self.config.kibana_headers:
            return self.config.kibana_headers
        return {}

    def request(self, tenant, method, url, **kwargs):
        session = self._make_session()
        full_url = f'{self.base_url}{url}'
        passed_headers = kwargs.get('headers', {})
        passed_headers.update(self._get_headers(tenant))
        kwargs['headers'] = passed_headers
        LOG.debug([method, full_url, kwargs])
        return session.request(method, full_url, **kwargs)

    def test(self, tenant):
        res = self.request(tenant, 'head', '')
        try:
            res.raise_for_status()
        except HTTPError as her:
            LOG.debug(f'Error testing kibana connection {her}')
            return False
        return True


class KibanaConnectionManager:
    # Collection of requests sessions pointing at Kibana instances

    def __init__(self):
        self.conn = {}

    def add_connection(
        self,
        config: KibanaConfig,
        tenant=None,
        instance=None
    ):
        conn = KibanaConnection(config)
        setattr(conn, 'instance_id', str(uuid4()))
        utils.replace_nested(self.conn, [tenant, instance], conn)

    def get_connection(self, tenant=None, instance=None):
        conn = self.conn.get(tenant, {}).get(instance)
        if not conn:
            raise ValueError(f'No matching instance {tenant}:{instance}')
        return conn


@dataclass
class ESConfig:
    '''Class an elasticsearch configuration'''
    elasticsearch_url: List[str]
    elasticsearch_user: str = None
    elasticsearch_password: str = None
    elasticsearch_port: int = None
    # set automatically depending on whether it's created with a Kibana
    has_kibana: bool = False


class ESConnectionManager:

    DEFAULT_TENANT = ':all'

    def __init__(self, add_default=True):
        self.conn = {ESConnectionManager.DEFAULT_TENANT: {}}
        self.config = {ESConnectionManager.DEFAULT_TENANT: {}}
        self.kibana = KibanaConnectionManager()
        if add_default:
            self._load_default_config()
            for x in range(CONN_RETRY):
                ok = self.test_connection()  # default
                if ok:
                    return
            LOG.critical('Could not connect to default ElasticSearch')
            sys.exit(1)

    def add_connection(
        self,
        config: ESConfig,
        tenant=DEFAULT_TENANT,
        instance='default',
        kibana_config: KibanaConfig = None
    ):
        # prepare connection details
        http_auth = [
            config.elasticsearch_user,
            config.elasticsearch_password
        ] if config.elasticsearch_user else None
        conn_info = {
            'port': config.elasticsearch_port,
            'http_auth': http_auth
        }
        conn_info = {k: v for k, v in conn_info.items() if v}
        conn_info['sniff_on_start'] = False
        # get connection
        LOG.debug(f'Adding ES connection: {config.elasticsearch_url}'
                  f'\n{json.dumps(conn_info, indent=2)}')
        conn = Elasticsearch(config.elasticsearch_url, **conn_info)
        # add an _id so we can check the instance
        setattr(conn, 'instance_id', str(uuid4()))

        if kibana_config:
            config.has_kibana = True
            self.kibana.add_connection(kibana_config, tenant, instance)
        # update values
        utils.replace_nested(self.config, [tenant, instance], config)
        utils.replace_nested(self.conn, [tenant, instance], conn)

    def _load_default_config(self):
        default_es = ESConfig(
            [CONSUMER_CONFIG.get('elasticsearch_url')],
            CONSUMER_CONFIG.get('elasticsearch_user'),
            CONSUMER_CONFIG.get('elasticsearch_password'),
            CONSUMER_CONFIG.get('elasticsearch_port')
        )
        if CONSUMER_CONFIG.get('kibana_password'):
            default_kibana = KibanaConfig(
                kibana_url=CONSUMER_CONFIG.get('kibana_url'),
                kibana_user=CONSUMER_CONFIG.get('kibana_user'),
                kibana_password=CONSUMER_CONFIG.get('kibana_password')
            )
        else:
            default_kibana = KibanaConfig(
                kibana_url=CONSUMER_CONFIG.get('kibana_url'),
                kibana_header_template='''
                {
                    "x-forwarded-for": "255.0.0.1",
                    "x-oauth-preferred_username":"aether-consumer",
                    "x-oauth-realm": "%s",
                    "kbn-xsrf": "f"
                }'''
            )
        self.add_connection(default_es, kibana_config=default_kibana)

    def get_connection(self, tenant=DEFAULT_TENANT, instance='default'):
        conn = self.conn.get(tenant, {}).get(instance)
        if not conn:
            raise ValueError(f'No matching instance {tenant}:{instance}')
        return conn

    def get_kibana(self, tenant=DEFAULT_TENANT, instance='default'):
        config = self.config.get(tenant, {}).get(instance)
        if not config or not config.has_kibana:
            return None
        return self.kibana.get_connection(tenant, instance)

    def test_connection(self, tenant=':all', instance='default'):
        conn = self.get_connection(tenant, instance)
        LOG.debug(f'Test connection {tenant}:{instance}')
        return ESConnectionManager.connection_is_live(conn)

    @staticmethod
    def connection_is_live(conn):
        try:
            es_info = conn.info()
            version = es_info.get('version').get('number')
            series = int(version.split('.')[0])
            if series not in SUPPORTED_ES_SERIES:
                LOG.error(f'Elastic Version {version} is not supported')
                return False
            return True
        except (
            NewConnectionError,
            ConnectionRefusedError,
            ESConnectionError
        ) as nce:
            LOG.error(f'Connection Error: {nce}')
            return False

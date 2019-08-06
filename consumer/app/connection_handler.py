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
import sys
from typing import Any, List, Mapping
from urllib3.exceptions import NewConnectionError

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


@dataclass
class ESConfig:
    '''Class an elasticsearch configuration'''
    elasticsearch_url: List[str]
    elasticsearch_user: str = None
    elasticsearch_password: str = None
    elasticsearch_port: int = None


class ESConnectionManager:

    DEFAULT_TENANT = ':all'

    def __init__(self, add_default=True):
        self.conn = {ESConnectionManager.DEFAULT_TENANT: {}}
        self.config = {ESConnectionManager.DEFAULT_TENANT: {}}
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
        instance='default'
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
        conn = Elasticsearch(config.elasticsearch_url, **conn_info)
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
        self.add_connection(default_es)

    def get_connection(self, tenant=DEFAULT_TENANT, instance='default'):
        conn = self.conn.get(tenant, {}).get(instance)
        if not conn:
            raise ValueError(f'No matching instance {tenant}:{instance}')
        return conn

    def test_connection(self, tenant=':all', instance='default'):
        conn = self.get_connection(tenant, instance)
        LOG.debug(f'Test connection {tenant}:{instance}')
        return self._test_connection(conn)

    def _test_connection(self, conn):
        try:
            es_info = conn.info()
            LOG.debug('ES Instance info: %s' % (es_info,))
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


@dataclass
class KibanaConfig:
    '''Class an elasticsearch configuration'''
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

        elif self.config.kibana_headers:
            return self.config.kibana_headers
        return {}

    def request(self, tenant, method, url, **kwargs):
        # options = copy(kwargs)
        session = self._make_session()
        full_url = f'{self.base_url}{url}'
        passed_headers = kwargs.get('headers', {})
        passed_headers.update(self._get_headers(tenant))
        kwargs['headers'] = passed_headers
        LOG.debug([method, full_url, kwargs])
        return session.request(method, full_url, **kwargs)


class KibanaConnectionManager:
    # Collection of requests sessions pointing at Kibana instances

    DEFAULT_TENANT = ':all'

    def __init__(self, add_default=True):
        self.conn = {KibanaConnectionManager.DEFAULT_TENANT: {}}
        if add_default:
            self._load_default_config()

    def _load_default_config(self):
        default = KibanaConfig(
            kibana_url=CONSUMER_CONFIG.get('kibana_url'),
            kibana_header_template='''{"x-oauth-realm": "%s","kbn-xsrf": "f"}'''
        )
        self.add_connection(default)

    def add_connection(
        self,
        config: KibanaConfig,
        tenant=DEFAULT_TENANT,
        instance='default'
    ):
        conn = KibanaConnection(config)
        utils.replace_nested(self.conn, [tenant, instance], conn)

    def get_connection(self, tenant=DEFAULT_TENANT, instance='default'):
        conn = self.conn.get(tenant, {}).get(instance)
        if not conn:
            raise ValueError(f'No matching instance {tenant}:{instance}')
        return conn

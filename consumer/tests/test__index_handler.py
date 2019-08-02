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
import pytest
import requests
import responses

from app import index_handler
from app.logger import LOG
from app.jsonpath import first, find

from . import *  # noqa  # fixtures


@responses.activate
@pytest.mark.unit
def test__handle_http():
    responses.add(
        responses.GET,
        'http://bad-url',
        json={'error': 'not found'},
        status=404
    )
    res = requests.get('http://bad-url')
    with pytest.raises(requests.exceptions.HTTPError):
        index_handler.handle_http(res)


@pytest.mark.unit
def test__get_es_index_from_autoconfig(AutoConfigSettings):
    ACS = AutoConfigSettings
    tenant = 'dev'
    name = 'a-topic'
    LOG.debug(json.dumps(ACS, indent=2))
    index = index_handler.get_es_index_from_autoconfig(
        ACS, name, tenant
    )
    LOG.debug(json.dumps(index, indent=2))
    idx_tmp = ACS['index_name_template']
    assert(first('$.name', index) == f'{tenant}.' + (idx_tmp % name))
    geo_name = ACS['geo_point_name']
    assert(first(f'$.body.mappings._doc.properties.{geo_name}', index) is not None)


@pytest.mark.unit
def test__get_index_for_topic(AutoConfigSettings):
    name = 'Person'
    geo_name = AutoConfigSettings.get('geo_point_name')
    auto_ts = AutoConfigSettings.get('auto_timestamp')
    index = index_handler.get_index_for_topic(name, geo_name, auto_ts)
    index = index.get('mappings', None)
    assert(len(index) == 1)
    assert(first('$._doc', index) is not None)
    assert(first(f'$._doc.properties.{geo_name}.type', index) == 'geo_point')
    assert(first(f'$._doc._meta.aet_auto_ts', index) == auto_ts)


@pytest.mark.unit
def test__register_es_artifacts():
    index = {'name': 'an_index'}
    with pytest.raises(ValueError):
        index_handler.register_es_artifacts()
    assert(index_handler.register_es_artifacts(index=index, mock=True))
    assert(index_handler.register_es_artifacts(
        index_path='/code/tests/test_index/es6',
        index_file='combined.json',
        mock=True
    ))


@pytest.mark.integration
def test__register_es_index():
    # TODO add test
    # Only ES functionality
    pass


def test__make_kibana_index(name):
    pass
    # def make_kibana_index(name):
    #     # throws HTTPError on failure
    #     host = consumer_config.get('kibana_url', None)
    #     if not host:
    #         LOG.debug('No kibana_url in config for default index creation.')
    #         return
    #     pattern = f'{name}*'
    #     index_url = f'{host}/api/saved_objects/index-pattern/{pattern}'
    #     headers = {'kbn-xsrf': 'meaningless-but-required'}
    #     kibana_ts = consumer_config.get('kibana_auto_timestamp', None)
    #     data = {
    #         'attributes': {
    #             'title': pattern,
    #             'timeFieldName': kibana_ts
    #         }
    #     }
    #     data['attributes']['timeFieldName'] = kibana_ts if kibana_ts else None
    #     LOG.debug(f'registering default kibana index: {data}')
    #     # register the base index
    #     handle_http(requests.post(index_url, headers=headers, json=data))
    #     default_url = f'{host}/api/kibana/settings/defaultIndex'
    #     data = {
    #         'value': pattern
    #     }
    #     # make this index the default
    #     handle_http(requests.post(default_url, headers=headers, json=data))
    #     LOG.debug(f'Created default index {pattern} on host {host}')


def test__index_from_file(index_path, index_file):
    pass
    # def index_from_file(index_path, index_file):
    #     index_name = index_file.split('.')[0]
    #     path = '%s/%s' % (index_path, index_file)
    #     with open(path) as f:
    #         return {
    #             'name': index_name,
    #             'body': json.load(f)
    #         }


def test__static_from_lookups(schema, fieldname, default="Other"):
    pass
    # def static_from_lookups(schema, fieldname, default="Other"):
    #     pass


def test__add_alias():
    pass
    # def add_alias():
    #     '''
    #     "aliases" : {}
    #     '''
    pass

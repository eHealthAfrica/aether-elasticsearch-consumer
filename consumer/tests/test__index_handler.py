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
from app.logger import get_logger
from app.jsonpath import first

from . import *  # noqa  # fixtures


LOG = get_logger('TEST-IDX')


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
    assert(first(
        f'$.body.mappings._doc.properties.{geo_name}', index) is not None)


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


# @pytest.mark.unit
# def test__register_es_artifacts():
#     index = {'name': 'an_index'}
#     with pytest.raises(ValueError):
#         index_handler.register_es_artifacts()
#     assert(index_handler.register_es_artifacts(index=index, mock=True))
#     assert(index_handler.register_es_artifacts(
#         index_path='/code/tests/test_index/es6',
#         index_file='combined.json',
#         mock=True
#     ))


@pytest.mark.integration
def test__register_es_index():
    # TODO add test
    # Only ES functionality
    pass


@pytest.mark.unit
def test__make_kibana_index(AutoGenSchema):
    name = 'kibana-index-name'
    res = index_handler.make_kibana_index(name, AutoGenSchema)
    assert(res.get('attributes', {}).get('title') == name)


@pytest.mark.unit
def test___find_timestamp(ComplexSchema):
    result = index_handler._find_timestamp(ComplexSchema)
    assert(result == 'timestamp')


@pytest.mark.unit
def test___format_lookups(ComplexSchema):
    formatted = index_handler._format_lookups(ComplexSchema)
    assert(
        json.dumps(
            formatted.get(
                'operational_status'), sort_keys=True) ==
        json.dumps(
            SAMPLE_FIELD_LOOKUP.get(
                'operational_status'), sort_keys=True)
    )


@pytest.mark.unit
def test___format_single_lookup(ComplexSchema):
    matching = ComplexSchema.get_node('Gth_Hs_Test_2.operational_status')
    res = index_handler._format_single_lookup(matching)
    assert(
        json.dumps(res, sort_keys=True) ==
        json.dumps(SAMPLE_FIELD_LOOKUP.get(
            'operational_status'), sort_keys=True)
    )


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


@pytest.mark.unit
def test__get_alias_from_namespace():
    tenant = 'test'
    namespace = 'A_Gather_Form_V1'
    res = index_handler.get_alias_from_namespace(tenant, namespace)
    assert(res == 'test.A_Gather_Form')

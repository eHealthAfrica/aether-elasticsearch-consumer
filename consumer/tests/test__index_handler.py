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

from aet.logger import get_logger


from app import index_handler

from . import *  # noqa  # fixtures


LOG = get_logger('TEST-IDX')


# convenience function for jsonpath

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
def test__get_es_index_from_autoconfig(SubscriptionDefinition, ComplexSchema):
    es_options = SubscriptionDefinition.get('es_options')
    tenant = 'dev'
    name = 'a-topic'
    alias = es_options.get('alias_name')
    index = index_handler.get_es_index_from_subscription(
        es_options, name, tenant, ComplexSchema
    )
    LOG.debug(json.dumps(index, indent=2))
    assert(first('$.name', index) == f'{tenant}.{name}')
    geo_name = es_options['geo_point_name']
    assert(first(
        f'$.body.mappings._doc.properties.{geo_name}', index) is not None)


@pytest.mark.unit
def test__get_index_for_topic(SubscriptionDefinition, ComplexSchema):
    name = 'Person'
    es_options = SubscriptionDefinition.get('es_options')
    geo_name = es_options.get('geo_point_name')
    auto_ts = es_options.get('auto_timestamp')
    index = index_handler.get_index_for_topic(name, geo_name, auto_ts, ComplexSchema)
    index = index.get('mappings', None)
    assert(len(index) == 1)
    assert(first('$._doc', index) is not None)
    assert(first(f'$._doc.properties.{geo_name}.type', index) == 'geo_point')
    assert(first(f'$._doc._meta.aet_auto_ts', index) == auto_ts)


@pytest.mark.unit
def test__get_es_types_from_schema(ComplexSchema):
    res = index_handler.get_es_types_from_schema(ComplexSchema)
    assert(first('$.beds.type', res) == 'integer')
    assert(first('$.username.type', res) == 'keyword')
    assert(first('$._start.type', res) == 'date')
    assert(first('$.geometry.type', res) == 'object')
    assert(first('$.meta.type', res) == 'object')
    assert(len(list(res.keys())) == 54)


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
    matching = ComplexSchema.get_node('MySurvey.operational_status')
    res = index_handler._format_single_lookup(matching)
    assert(
        json.dumps(res, sort_keys=True) ==
        json.dumps(SAMPLE_FIELD_LOOKUP.get(
            'operational_status'), sort_keys=True)
    )


@pytest.mark.unit
def test__get_alias_from_namespace():
    namespace = 'A_Gather_Form_V1'
    res = index_handler.get_alias_from_namespace(namespace)
    assert(res == 'A_Gather_Form')

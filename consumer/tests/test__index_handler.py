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

# import json
import pytest
import requests
import responses

from app import index_handler

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


def test__get_es_index_from_autoconfig():
    pass
    # def get_es_index_from_autoconfig(
    #     autoconf,
    #     topic_name,
    #     MT=True
    # ):
    #     geo_point = (
    #         autoconf.get('geo_point_name', None)
    #         if autoconf.get('geo_point_creation', False)
    #         else None
    #     )
    #     auto_ts = autoconf.get('auto_timestamp', None)
    #     if MT:
    #         tenant = topic_name.split('.')[0]
    #         _name = '.'.join(topic_name.split('.')[1:])
    #         index_name = (
    #             tenant +
    #             '.' +
    #             autoconf.get('index_name_template') % _name).lower()
    #     else:
    #         index_name = (autoconf.get('index_name_template') % topic_name).lower()
    #     LOG.debug('Index name => %s' % index_name)
    #     index = {
    #         'name': index_name,
    #         'body': get_index_for_topic(
    #             topic_name, geo_point, auto_ts
    #         )
    #     }
    #     return index


def test__get_index_for_topic(AutoConfigSettings):
    name = 'Person'
    geo_name = AutoConfigSettings.get('geo_point_name')
    index = index_handler.get_index_for_topic(name, geo_name)
    index = index.get('mappings', None)
    assert(len(index) is 1)
    assert(index.get('_doc') is not None)
    assert(index.get('_doc').get('properties').get(geo_name) is not None)
    assert(index.get('_doc').get('properties').get(geo_name).get('type') is 'geo_point')

    # def get_index_for_topic(name, geo_point=None, auto_ts=None):
    #     LOG.debug('Creating mappings for topic %s' % name)
    #     mappings = {
    #         # name: {
    #         #     '_meta': {
    #         #         'aet_subscribed_topics': [name]
    #         #     }
    #         # }
    #         '_doc': {  # 7.x has made names illegal here...
    #             '_meta': {
    #                 'aet_subscribed_topics': [name]
    #             }
    #         }
    #     }
    #     if geo_point:
    #         mappings['_doc']['_meta']['aet_geopoint'] = geo_point
    #         mappings['_doc']['properties'] = {geo_point: {'type': 'geo_point'}}
    #     if auto_ts:
    #         mappings['_doc']['_meta']['aet_auto_ts'] = auto_ts
    #     LOG.debug('created mappings: %s' % mappings)
    #     return {'mappings': mappings}


def test__register_es_index(es, index_path=None, index_file=None, index=None):
    pass
    # # TODO Add Kibana Index handling here...
    # def register_es_index(es, index_path=None, index_file=None, index=None):
    #     if not any([index_path, index_file, index]):
    #         raise ValueError('Index cannot be created without an artifact')
    #     if index_path and index_file:
    #         LOG.debug('Loading index %s from file: %s' % (index_file, index_path))
    #         index = index_from_file(index_path, index_file)
    #     index_name = index.get('name')
    #     if es.indices.exists(index=index.get('name')):
    #         LOG.debug('Index %s already exists, skipping creation.' % index_name)
    #     else:
    #         LOG.info('Creating Index %s' % index.get('name'))
    #         es.indices.create(
    #             index=index_name,
    #             body=index.get('body'),
    #             params={'include_type_name': 'true'}  # json true...
    #         )


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

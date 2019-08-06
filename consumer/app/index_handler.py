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
import requests

from .import config
from .logger import get_logger
from .schema import Node

LOG = get_logger('INDEX')
consumer_config = config.get_consumer_config()
kafka_config = config.get_kafka_config()


def handle_http(req):
    req.raise_for_status()


def get_es_index_from_autoconfig(
    autoconf,
    name=None,
    tenant=None
):
    geo_point = (
        autoconf.get('geo_point_name', None)
        if autoconf.get('geo_point_creation', False)
        else None
    )
    auto_ts = autoconf.get('auto_timestamp', None)
    index_name = (autoconf.get('index_name_template') % name).lower()
    topic_name = f'{tenant}.{name}'
    index_name = f'{tenant}.{index_name}'.lower()
    index = {
        'name': index_name,
        'body': get_index_for_topic(
            topic_name, geo_point, auto_ts
        )
    }
    return index


def get_index_for_topic(name, geo_point=None, auto_ts=None):
    LOG.debug('Creating mappings for topic %s' % name)
    mappings = {
        # name: {
        #     '_meta': {
        #         'aet_subscribed_topics': [name]
        #     }
        # }
        '_doc': {  # 7.x has made names illegal here...
            '_meta': {
                'aet_subscribed_topics': [name]
            }
        }
    }
    if geo_point:
        mappings['_doc']['_meta']['aet_geopoint'] = geo_point
        mappings['_doc']['properties'] = {geo_point: {'type': 'geo_point'}}
    if auto_ts:
        mappings['_doc']['_meta']['aet_auto_ts'] = auto_ts
    LOG.debug('created mappings: %s' % mappings)
    return {'mappings': mappings}


# # TODO Add Kibana Index handling here...
# def register_es_artifacts(
#     es=None,
#     index_path=None,
#     index_file=None,
#     index=None,
#     mock=False
# ):
#     if not any([index_path, index_file, index]):
#         raise ValueError('Index cannot be created without an artifact')
#     if index_path and index_file:
#         LOG.debug('Loading index %s from file: %s' % (index_file, index_path))
#         index = index_from_file(index_path, index_file)
#     if mock:
#         return True
#     register_es_index(es, index)


def register_es_index(es, index):
    index_name = index.get('name')
    if es.indices.exists(index=index.get('name')):
        LOG.debug('Index %s already exists, skipping creation.' % index_name)
        return False
    else:
        # TODO Add alias
        LOG.info('Creating Index %s' % index.get('name'))
        es.indices.create(
            index=index_name,
            body=index.get('body'),
            params={'include_type_name': 'true'}  # json true...
        )
        return True


def make_kibana_index(tenant, name, schema):
    kibana_ts = consumer_config.get('kibana_auto_timestamp', None)
    data = {
        'attributes': {
            'title': name,
            'timeFieldName': kibana_ts
        }
    }
    data['attributes']['timeFieldName'] = kibana_ts if kibana_ts else None
    return data


def _format_lookups(schema, default='Other'):
    schema = Node(schema)
    matching = schema.collect_matching(
        {'has_attr': ['__lookup']}
    )
    if not matching:
        return {}
    return {
        key: _format_single_lookup(node, default)
        for key, node in matching
    }


def _format_single_lookup(node, default='Other'):
    lookup = node.__lookup
    definition = {
        'id': 'static_lookup',
        'params': {'lookupEntries': [
            {'value': pair['label'], 'key': pair['value']} for pair in lookup
        ], 'unknownKeyValue': default}
    }
    return definition


def register_kibana_index(name, index, tenant):
    # throws HTTPError on failure
    host = consumer_config.get('kibana_url', None)
    if not host:
        LOG.debug('No kibana_url in config for default index creation.')
        return
    pattern = f'{name}'
    index_url = f'{host}/api/saved_objects/index-pattern/{pattern}'
    headers = {
        'x-oauth-realm': tenant,  # tenant to create on behalf of
        'kbn-xsrf': 'f'  # meaningless but required
    }
    LOG.debug(f'registering kibana index: {index}')
    # register the index
    handle_http(requests.post(index_url, headers=headers, json=index))


def index_from_file(index_path, index_file):
    index_name = index_file.split('.')[0]
    path = '%s/%s' % (index_path, index_file)
    with open(path) as f:
        return {
            'name': index_name,
            'body': json.load(f)
        }


def add_alias():
    '''
    "aliases" : {}
    '''
    pass

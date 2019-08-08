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
from typing import Any, Mapping

from requests.exceptions import HTTPError

from .import config
from .logger import get_logger
from .schema import Node
from . import utils

from .connection_handler import KibanaConnection

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


def register_es_index(es, index, alias=None):
    index_name = index.get('name')
    if es.indices.exists(index=index.get('name')):
        LOG.debug('Index %s already exists, skipping creation.' % index_name)
        return False
    else:
        LOG.info('Creating Index %s' % index.get('name'))
        es.indices.create(
            index=index_name,
            body=index.get('body'),
            params={'include_type_name': 'true'}  # json true...
        )
        if alias:
            es.indices.put_alias(index=index_name, name=alias)
        return True


def get_alias_from_namespace(tenant: str, namespace: str):
    parts = namespace.split('_')
    if len(parts) < 2:
        return f'{tenant}.{namespace}'
    return f'{tenant}.' + '_'.join(parts[:-1])


def make_kibana_index(name, schema: Node):
    lookups = _format_lookups(schema)
    data = {
        'attributes': {
            'title': name,
            'timeFieldName': _find_timestamp(schema),
            'fieldFormatMap': json.dumps(  # Kibana requires this be escaped
                lookups,
                sort_keys=True
            ) if lookups else None  # Don't include if there aren't any
        }
    }
    return data


def kibana_handle_schema_change(
    tenant: str,
    alias: str,
    schema_old: Mapping[Any, Any],
    schema_new: Mapping[Any, Any],
    es_index: Mapping[Any, Any],
    es_conn,
    kibana_conn
):
    node_new = Node(schema_new)
    kibana_index = make_kibana_index(alias, node_new)
    if schema_old is not None:
        node_old = Node(schema_old)
        if Node.compare(node_old, node_new) == {}:
            return False  # schema not substantially different
    if not check_for_kibana_update(
            tenant,
            alias,
            kibana_index,
            es_index,
            es_conn,
            kibana_conn):
        return False

    return update_kibana_index(
        tenant,
        alias,
        kibana_index,
        es_index,
        es_conn,
        kibana_conn
    )


def check_for_kibana_update(
    tenant: str,
    alias: str,
    kibana_index: Mapping[Any, Any],
    es_index: Mapping[Any, Any],
    es_conn,
    kibana_conn
):
    # if the schema is unchanged, we don't need to do anything.
    index_hash = utils.hash(kibana_index)
    artifact = get_es_artifact_for_alias(alias, tenant, es_conn)
    try:
        old_kibana_index = handle_kibana_index(
            alias,
            tenant,
            kibana_conn,
            mode='READ'
        )
    except HTTPError as hpe:
        LOG.debug(f'Could not get old kibana index: {hpe}')
        old_kibana_index = {}
    LOG.debug(json.dumps({
        'i_hash': index_hash,
        'art': artifact,
        'old_kibana_index': old_kibana_index
    }, indent=2))

    if not artifact or old_kibana_index:
        return True
    # TODO update...
    return False


def update_kibana_index(
    tenant: str,
    alias: str,
    kibana_index: Mapping[Any, Any],
    es_index: Mapping[Any, Any],
    es_conn,
    kibana_conn
):
    # find differences between indices
    # create new asset

    old_artifact = get_es_artifact_for_alias(alias, tenant, es_conn)
    merged_index, new_artifact = merge_kibana_artifacts(
        kibana_index,
        es_conn,
        old_artifact
    )
    try:
        try:
            handle_kibana_index(alias, tenant, kibana_conn, merged_index, 'CREATE')
        except HTTPError:
            handle_kibana_index(alias, tenant, kibana_conn, merged_index, 'UPDATE')
        finally:
            put_es_artifact_for_alias(alias, tenant, new_artifact, es_conn)
    except Exception as err:
        LOG.critical(f'Error registering Kibana App: {err}')
    # save asset hashes

    pass


def _remove_formname(name):
    pieces = name.split('.')
    return '.'.join(pieces[1:])


def _find_timestamp(schema: Node):
    # takes a field matching timestamp, or the first timestamp
    matching = schema.collect_matching(
        {'match_attr': [{'__extended_type': 'dateTime'}]}
    )
    fields = sorted([key for key, node in matching])
    timestamps = [f for f in fields if 'timestamp' in f]
    if timestamps:
        return _remove_formname(timestamps[0])
    elif fields:
        return _remove_formname(fields[0])
    else:
        return consumer_config.get(
            'autoconfig_settings', {}).get(
            'auto_timestamp', None)


def _format_lookups(schema: Node, default='Other', strip_form_name=True):
    matching = schema.collect_matching(
        {'has_attr': ['__lookup']}
    )
    if not matching:
        return {}
    if not strip_form_name:
        return {
            key: _format_single_lookup(node, default)
            for key, node in matching
        }
    else:
        return {
            _remove_formname(key): _format_single_lookup(node, default)
            for key, node in matching
        }


def _format_single_lookup(node: Node, default='Other'):
    lookup = node.__lookup
    definition = {
        'id': 'static_lookup',
        'params': {'lookupEntries': [
            {'value': pair['label'], 'key': pair['value']} for pair in lookup
        ], 'unknownKeyValue': default}
    }
    return definition


def merge_kibana_artifacts(kibana_index, es, old_artifact=None):
    visualizations = {}  # make_visualizations
    new_artifact = {
        'hashes': {
            'index': {
                'utils': utils.hash(kibana_index)
            },
            'template': {k: utils.hash(v) for k, v in visualizations}
        }
    }
    if old_artifact:
        merged_index = kibana_index
    else:
        # TODO merge indices based on diff in artifacts
        merged_index = kibana_index
    return merged_index, new_artifact


def __get_es_artifact_index_name(tenant):
    return f'{tenant}._aether_artifacts_v1'.lower()


def __get_es_artifact_doc_name(alias):
    return f'kibana.{alias}'


def get_es_artifact_for_alias(alias, tenant, es):
    index = __get_es_artifact_index_name(tenant)
    _id = __get_es_artifact_doc_name(alias)
    if es.exists(index=index, id=_id):
        return es.get(index=index, id=_id)
    LOG.debug(f'No artifact doc for {_id}')
    return None


def make_es_artifact_index(tenant, es):
    index_name = __get_es_artifact_index_name(tenant)
    if not es.indices.exists(index_name):
        LOG.debug(f'Creating artifact index {index_name} for tenant {tenant}')
        es.indices.create(index=index_name)
    return


def put_es_artifact_for_alias(name, tenant, doc, es):
    index = __get_es_artifact_index_name(tenant)
    _id = __get_es_artifact_doc_name(name)
    make_es_artifact_index(tenant, es)  # make sure we have an index
    if not es.exists(index=index, id=_id):
        LOG.debug(f'Creating ES Artifact for {tenant}:{name}')
        es.create(
            index=index,
            id=_id,
            body=doc
        )
    else:
        LOG.debug(f'Updating ES Artifact for {tenant}:{name}')
        es.update(
            index=index,
            id=_id,
            body=doc
        )


def handle_kibana_index(name, tenant, conn: KibanaConnection, index=None, mode='CREATE'):

    modes = {
        'CREATE': 'post',
        'READ': 'get',
        'UPDATE': 'put',
        'DELETE': 'delete'
    }
    operation = modes.get(mode)
    if not operation:
        raise ValueError(f'Unknown request type: {mode}')
    payload = None
    if mode in ['CREATE', 'UPDATE']:
        payload = index
    pattern = f'{name}'
    index_url = f'/api/saved_objects/index-pattern/{pattern}'
    res = conn.request(tenant, operation, index_url, json=payload)
    handle_http(res)
    if mode == 'READ':
        return res.json()
    return res


def index_from_file(index_path, index_file):
    index_name = index_file.split('.')[0]
    path = '%s/%s' % (index_path, index_file)
    with open(path) as f:
        return {
            'name': index_name,
            'body': json.load(f)
        }

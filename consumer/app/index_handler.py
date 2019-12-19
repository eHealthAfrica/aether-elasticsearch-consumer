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

from requests import Session
from requests.exceptions import HTTPError

from aet.logger import get_logger
from aether.python.avro.schema import Node

from .import config
from .processor import ES_RESERVED
from .visualization import (
    auto_visualizations, schema_defined_visualizations
)
from . import utils

LOG = get_logger('INDEX')
consumer_config = config.get_consumer_config()
kafka_config = config.get_kafka_config()


def handle_http(req):
    req.raise_for_status()


def get_es_index_from_subscription(
    es_options,
    name=None,
    tenant=None,
    schema: Node = None
):
    geo_point = (
        es_options.get('geo_point_name', None)
        if es_options.get('geo_point_creation', False)
        else None
    )
    auto_ts = es_options.get('auto_timestamp', None)
    topic_name = f'{tenant}.{name}'
    index_name = f'{tenant}.{name}'.lower()
    index = {
        'name': index_name,
        'body': get_index_for_topic(
            topic_name, geo_point, auto_ts, schema
        )
    }
    return index


def get_index_for_topic(
    name,
    geo_point=None,
    auto_ts=None,
    schema: Node = None
):
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
    mappings['_doc']['properties'] = get_es_types_from_schema(schema)
    if geo_point:
        mappings['_doc']['_meta']['aet_geopoint'] = geo_point
        mappings['_doc']['properties']['geo_point'] = {'type': 'geo_point'}
    if auto_ts:
        mappings['_doc']['_meta']['aet_auto_ts'] = auto_ts
    LOG.debug('created mappings: %s' % mappings)
    return {'mappings': mappings}


def get_es_types_from_schema(schema: Node):
    # since we handle union types, we sort these in increasing importance
    # to ES's handling of them. I.E. if it can be an object or a string,
    # it's more tolerant to treat it as an object, etc.

    mappings = {}

    for avro_type, es_type in config.AVRO_TYPES:
        matches = [i for i in schema.find_children(
            {'attr_contains': [{'avro_type': avro_type}]})
        ]
        for match in matches:
            path = remove_formname(match)
            if path and path not in ES_RESERVED:
                mappings[path] = {'type': es_type}

    for aether_type, es_type in config.AETHER_TYPES:
        matches = [i for i in schema.find_children(
            {'match_attr': [{'__extended_type': aether_type}]})
        ]
        for match in matches:
            path = remove_formname(match)
            if path and path not in ES_RESERVED:
                mappings[path] = {'type': es_type}

    return mappings


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


def get_alias_from_namespace(namespace: str):
    parts = namespace.split('_')
    if len(parts) < 2:
        return f'{namespace}'
    return '_'.join(parts[:-1])


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
    alias_name: str,
    schema_old: Mapping[Any, Any],
    schema_new: Mapping[Any, Any],
    subscription: Mapping[str, Any],  # Subscription.definition
    es_index: Mapping[Any, Any],
    es_conn,
    kibana_conn
):
    node_new = Node(schema_new)
    kibana_index = make_kibana_index(alias_name, node_new)
    schema_name = schema_new.get('name')
    if schema_old is not None:
        if schema_old.get('name'):
            schema_name = schema_old.get('name')
        node_old = Node(schema_old)
        if Node.compare(node_old, node_new) == {}:
            return False  # schema not substantially different
    if not check_for_kibana_update(
            schema_name,
            tenant,
            alias_name,
            subscription,
            kibana_index,
            es_index,
            es_conn,
            kibana_conn):
        return False

    return update_kibana_index(
        tenant,
        alias_name,
        schema_new,
        subscription,
        kibana_index,
        es_index,
        es_conn,
        kibana_conn
    )


def check_for_kibana_update(
    schema_name: str,
    tenant: str,
    alias: str,
    subscription: Mapping[str, Any],  # Subscription.definition
    kibana_index: Mapping[Any, Any],
    es_index: Mapping[Any, Any],
    es_conn,
    kibana_conn
):
    # if the schema is unchanged, we don't need to do anything.
    index_hash = utils.hash(kibana_index)
    artifact = get_es_artifact_for_alias(alias, tenant, es_conn)
    try:
        old_kibana_index = handle_kibana_artifact(
            alias,
            tenant,
            kibana_conn,
            mode='READ',
            _type='index-pattern'
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
    old_index_hash = artifact.get(
        'hashes', {}).get(
        'index', {}).get(
        schema_name
    )
    if old_index_hash != index_hash:
        return True
    return False


def update_kibana_index(
    tenant: str,
    alias_name: str,
    schema: Mapping[Any, Any],
    subscription: Mapping[str, Any],  # Subscription.definition
    kibana_index: Mapping[Any, Any],
    es_index: Mapping[Any, Any],
    es_conn,
    kibana_conn
):
    # find differences between indices
    # create new asset

    old_artifact = get_es_artifact_for_alias(alias_name, tenant, es_conn)
    merged_index, new_artifact, updated_visuals = merge_kibana_artifacts(
        tenant,
        alias_name,
        schema,
        subscription,
        kibana_index,
        kibana_conn,
        old_artifact
    )
    if not any([merged_index, new_artifact, updated_visuals]):
        LOG.debug('No kibana update required')
        return
    try:
        update_kibana_artifact(
            alias_name,
            tenant,
            kibana_conn,
            merged_index,
            'index-pattern'
        )
        for vis_id, body in updated_visuals.items():
            update_kibana_artifact(
                vis_id,
                tenant,
                kibana_conn,
                body,
                'visualization'
            )
        # save the new hashes last in case of partial failure
        # on restart, it should try again
        put_es_artifact_for_alias(alias_name, tenant, new_artifact, es_conn)
        default_index = get_default_index(tenant, kibana_conn)
        if not default_index:
            LOG.debug(f'No default index is set, using: {alias_name}')
            set_default_index(tenant, kibana_conn, alias_name)
        else:
            LOG.debug(
                f'default index {default_index} already set. Ignoring.')
        return True
    except HTTPError as her:
        LOG.critical(f'Kibana index update failed: {her}')
        raise her
    except Exception as err:
        LOG.critical(f'Kibana index with generic: {err}')
        raise err


def remove_formname(name):
    pieces = name.split('.')
    return '.'.join(pieces[1:])


def get_formname(name):
    return name.split('.')[0]


def _find_timestamp(schema: Node):
    # takes a field matching timestamp, or the first timestamp
    matching = schema.collect_matching(
        {'match_attr': [{'__extended_type': 'dateTime'}]}
    )
    fields = sorted([key for key, node in matching])
    timestamps = [f for f in fields if 'timestamp' in f]
    if timestamps:
        return remove_formname(timestamps[0])
    elif fields:
        return remove_formname(fields[0])
    else:
        return consumer_config.get(
            'es_optionsig_settings', {}).get(
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
            remove_formname(key): _format_single_lookup(node, default)
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


def _make_artifact(index=None, visualization=None, old_artifact=None):
    indices = {
        k: v for k, v in index.items()} \
        if isinstance(index, dict)  \
        else {}

    visualizations = {
        k: v for k, v in visualization.items()} \
        if isinstance(visualization, dict) \
        else {}

    new_artifact = {
        'hashes': {
            'index': indices,
            'visualization': visualizations
        }
    }
    if not old_artifact:
        return new_artifact
    _source = old_artifact.get('_source', {})
    return utils.merge_dicts(_source, new_artifact)


def merge_kibana_artifacts(
    tenant: str,
    alias_name: str,
    schema: Mapping[Any, Any],
    subscription: Mapping[str, Any],  # Subscription.definition
    kibana_index: Mapping[Any, Any],  # individual kibana index contribution
    kibana_conn,
    old_artifact: Mapping[Any, Any] = None  # artifact describes multiple types
):
    schema_name = schema.get('name')
    index_hash = utils.hash(kibana_index)
    # TODO
    alias_index = f'{tenant}.{alias_name}'
    auto_viz_flag = subscription.get('kibana_options', {}).get('auto_vizualization')
    if auto_viz_flag == 'full':
        LOG.info('Creating automatic visualizations')
        visualizations = auto_visualizations(
            alias_name,
            alias_index,
            Node(schema)
        )
    elif auto_viz_flag == 'schema':
        LOG.info('Only creating vis from @aether_default_visualization')
        visualizations = schema_defined_visualizations(
            alias_name,
            alias_index,
            Node(schema)
        )
    else:
        LOG.info('Not creating visualizations')
        visualizations = {}
    vis_hashes = {k: utils.hash(v) for k, v in visualizations.items()}
    if not old_artifact:
        # use the new one since there is no old one

        artifact = _make_artifact(
            index={schema_name: index_hash},
            visualization=vis_hashes
        )
        return kibana_index, artifact, visualizations
    old_index_hash = old_artifact.get(
        'hashes', {}).get(
        'index', {}).get(
        schema_name
    )
    old_vis_hashes = old_artifact.get(
        'hashes', {}).get(
        'visualization', {}
    )
    updated_visuals = {
        key: visualizations[key] for key, _hash in vis_hashes.items()
        if _hash not in old_vis_hashes.values()
    }
    if updated_visuals:
        LOG.debug(f'updated visuals: {list(updated_visuals.keys())}')
    # no change, ignore
    if (old_index_hash == index_hash) and (len(updated_visuals) == 0):
        return None, None, None

    # we need to reconcile the update
    old_kibana_index = handle_kibana_artifact(
        alias_name,
        tenant,
        kibana_conn,
        mode='READ',
        _type='index-pattern'
    )
    new_kibana_index = utils.merge_dicts(old_kibana_index, kibana_index)
    artifact = _make_artifact(
        index={schema_name: index_hash},
        visualization=vis_hashes,
        old_artifact=old_artifact
    )
    return new_kibana_index, artifact, updated_visuals


def __get_es_artifact_index_name(tenant):
    return f'{tenant}._aether_artifacts_v1'.lower()


def __get_es_artifact_doc_name(alias):
    return f'kibana.{alias}'


def get_es_artifact_for_alias(alias, tenant, es):
    index = __get_es_artifact_index_name(tenant)
    _id = __get_es_artifact_doc_name(alias)
    if es.exists(index=index, id=_id):
        doc = es.get(index=index, id=_id)
        return doc.get('_source', {})
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
        LOG.debug(f'Updating ES Artifact for {tenant}:{name}:{_id}')
        LOG.debug(json.dumps(doc, indent=2))
        es.update(
            index=index,
            id=_id,
            body={'doc': doc}
        )


def update_kibana_artifact(
    alias_name,
    tenant,
    conn:
    Session,
    index=None,
    _type='index-pattern'
):
    try:
        handle_kibana_artifact(
            alias_name,
            tenant,
            conn,
            index,
            'CREATE',
            _type
        )
    except HTTPError:
        handle_kibana_artifact(
            alias_name,
            tenant,
            conn,
            index,
            'UPDATE',
            _type
        )


def handle_kibana_artifact(
    alias_name,
    tenant,
    conn: Session,
    index=None,
    mode='CREATE',
    _type=None
):
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
    pattern = f'{tenant}.{alias_name}'
    index_url = f'/api/saved_objects/{_type}/{pattern}'
    res = conn.request(operation, index_url, json=payload)
    try:
        handle_http(res)
    except HTTPError as her:
        LOG.info(f'Kibana index handle failed op: {operation}:{res.status_code}')
        LOG.debug(res.text)
        LOG.debug(f'index: {json.dumps(index, indent=2)}')
        raise her
    if mode == 'READ':
        body = res.json()
        # Only the attributes fields can be passed back,
        # so we remove the others here
        return {'attributes': body.get('attributes', {})}
    return res


def get_default_index(tenant, conn: Session):
    url = '/api/kibana/settings'
    op = 'get'
    res = conn.request(op, url)
    try:
        handle_http(res)
        default = res.json().get('settings', {}) \
            .get('defaultIndex', {}) \
            .get('userValue')
        return default
    except HTTPError as her:
        LOG.debug(f'Could not get default index: {her}')
        return None


def set_default_index(tenant, conn: Session, index_name):
    url = '/api/kibana/settings/defaultIndex'
    op = 'post'
    res = conn.request(op, url, json={'value': index_name})
    try:
        handle_http(res)
        return True
    except HTTPError as her:
        LOG.debug(f'Could not set default index to {index_name}: {her}')
        return False


def index_from_file(index_path, index_file):
    index_name = index_file.split('.')[0]
    path = '%s/%s' % (index_path, index_file)
    with open(path) as f:
        return {
            'name': index_name,
            'body': json.load(f)
        }

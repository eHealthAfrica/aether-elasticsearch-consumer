#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
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

from datetime import datetime, timedelta
import json

from aet.logger import get_logger
from aet.jsonpath import CachedParser
from aether.python.utils import replace_nested
from aether.python.avro.schema import Node

from .config import get_consumer_config, AVRO_TYPES


LOG = get_logger('PROCESS')
CONSUMER_CONFIG = get_consumer_config()


ES_RESERVED = [
    '_uid', '_id', '_type', '_source', '_all', '_field_names',
    '_routing', '_index', '_size', '_timestamp', '_ttl', '_version'
]

AVRO_BASE_COERSCE = {
    # avro_type -> handler
}

AVRO_LOGICAL_COERSCE = {
    # logical_avro_type -> handler
    # int(days since epoch) -> iso_string
    'date': lambda x: (
        (datetime(1970, 1, 1) + timedelta(days=x)).isoformat())[:10]
}


class ESItemProcessor(object):

    @staticmethod
    def _get_doc_field(doc, name):
        if not name:
            raise ValueError('Invalid field name')
        doc = json.loads(json.dumps(doc))
        try:
            matches = CachedParser.find(name, doc)
            if not matches:
                raise ValueError(name, doc)
            if len(matches) > 1:
                LOG.warn(f'More than one value for {name} using first')
            return matches[0].value
        except ValueError as ve:
            LOG.debug(f'Error getting field {name}')
            raise ve

    @staticmethod
    def _coersce_field(doc, field_path=None, trans_fn=None, **kwargs):
        try:
            field_value = ESItemProcessor._get_doc_field(doc, field_path)
            value = trans_fn(field_value)
            replace_nested(doc, field_path.split('.'), value, False)
            return doc
        except Exception as err:
            LOG.error(f'could not coersce field {field_path}: {err}')
            return doc

    @staticmethod
    def _most_permissive_avro_type(_types):
        if not isinstance(_types, list):
            return _types
        try:
            return [_type for _type, _ in AVRO_TYPES if _type in _types][-1]
        except IndexError:
            return None

    def __init__(self, type_name, type_instructions, schema: Node):
        self.pipeline = []
        self.schema = schema
        self.es_type = type_name
        self.type_instructions = type_instructions
        self.topic_name = type_name
        self.has_parent = False
        self.load()

    def load(self):
        self.pipeline = []
        self.has_parent = False
        meta = self.type_instructions.get('_meta')
        if not meta:
            LOG.debug(f'type: {self.es_type} has no meta arguments')
            return
        for key, value in meta.items():
            LOG.debug(f'Type {self.es_type} has meta type: key')
            if key == 'aet_parent_field':
                join_field = meta.get('aet_join_field', None)
                if join_field:  # ES 6
                    field = value.get(self.es_type)
                else:  # ES 5
                    field = value
                if field and join_field:
                    self.has_parent = True
                    cmd = {
                        'function': '_add_parent',
                        'field_name': field,
                        'join_field': join_field
                    }
                    self.pipeline.append(cmd)
            elif key == 'aet_geopoint':
                cmd = {
                    'function': '_add_geopoint',
                    'field_name': value
                }
                try:
                    cmd.update(self._find_geopoints())
                    self.pipeline.append(cmd)
                except ValueError as ver:
                    LOG.error(f'In finding geopoints in pipeline {self.es_type} : {ver}')
            elif key == 'aet_auto_ts':
                cmd = {
                    'function': '_add_timestamp',
                    'field_name': value
                }
                self.pipeline.append(cmd)
            elif key.startswith('aet'):
                LOG.debug(f'aet _meta keyword {key} in type {self.es_type} generates no command')
            else:
                LOG.debug(f'Unknown meta keyword {key} in type {self.es_type}')
        for full_path in self.schema.iter_children():
            _base_name = f'{self.schema.name}.'
            if full_path.startswith(_base_name):
                path = full_path[len(_base_name):]
            else:
                path = full_path
            child = self.schema.get_node(full_path)
            permissive_type = self._most_permissive_avro_type(child.avro_type)
            if hasattr(child, 'logical_type') and child.logical_type in AVRO_LOGICAL_COERSCE:
                cmd = {
                    'function': '_coersce_field',
                    'field_path': path,
                    'trans_fn': AVRO_LOGICAL_COERSCE[child.logical_type]
                }
                self.pipeline.append(cmd)
            elif permissive_type in AVRO_BASE_COERSCE:
                cmd = {
                    'function': '_coersce_field',
                    'field_path': path,
                    'trans_fn': AVRO_BASE_COERSCE[permissive_type]
                }
        LOG.debug(f'Pipeline for {self.es_type}: {self.pipeline}')

    def create_route(self):
        meta = self.type_instructions.get('_meta', {})
        join_field = meta.get('aet_join_field', None)
        if not self.has_parent or not join_field:
            LOG.debug(f'NO Routing created for type {self.es_type}')
            return lambda *args: None

        def route(doc):
            return doc.get(join_field).get('parent')
        LOG.debug(f'Routing created for child type {self.es_type}')
        return route

    def rename_reserved_fields(self, doc):
        for key in doc:
            if key in ES_RESERVED:
                val = self._get_doc_field(doc, key)
                safe_name = 'es_reserved_%s' % key
                doc[safe_name] = val
                del doc[key]
        return doc

    def process(self, doc, schema=None):
        # Runs the cached insturctions from the built pipeline
        for instr in self.pipeline:
            doc = self.exc(doc, instr)
        doc = self.rename_reserved_fields(doc)
        return doc

    def exc(self, doc, instr):
        # Excecute by name
        fn = getattr(self, instr.get('function'))
        return fn(doc, **instr)

    def _add_parent(self, doc, field_name=None, join_field=None, **kwargs):
        try:
            payload = {
                'name': self.es_type,
                'parent': self._get_doc_field(doc, field_name)
            }
            doc[join_field] = payload
        except Exception as e:
            LOG.error(f'Could not add parent to doc type {self.es_type}. '
                      f'Error: {e}')
        return doc

    def _add_geopoint(self, doc, field_name=None, lat=None, lon=None, **kwargs):
        geo = {}
        try:
            geo['lat'] = float(self._get_doc_field(doc, lat))
            geo['lon'] = float(self._get_doc_field(doc, lon))
            doc[field_name] = geo
        except Exception as e:
            LOG.debug(f'Could not add geo to doc type {self.es_type}. '
                      f'Error: {e} | {lat} {lon}')
        return doc

    def _add_timestamp(self, doc, field_name=None, **kwargs):
        doc[field_name] = str(datetime.now().isoformat())
        return doc

    def _find_geopoints(self):
        res = {}
        latitude_fields = CONSUMER_CONFIG.get('latitude_fields')
        longitude_fields = CONSUMER_CONFIG.get('longitude_fields')
        LOG.debug(f'looking for matches in {latitude_fields} & {longitude_fields}')
        for lat in latitude_fields:
            path = self.find_path_in_schema(self.schema, lat)
            if path:
                res['lat'] = path[0]  # Take the first Lat
                break
        for lng in longitude_fields:
            path = self.find_path_in_schema(self.schema, lng)
            if path:
                res['lon'] = path[0]  # Take the first Lng
                break
        if 'lat' not in res or 'lon' not in res:
            raise ValueError('Could not resolve geopoints for field %s of type %s' % (
                'location', self.es_type))
        return res

    def find_path_in_schema(self, schema: Node, test):
        _base_name = f'{schema.name}.'
        matches = [
            i[len(_base_name):]
            if i.startswith(_base_name)
            else i
            for i in schema.find_children(
                {'match_attr': [{'name': test}]}
            )
        ]
        return matches if matches else []

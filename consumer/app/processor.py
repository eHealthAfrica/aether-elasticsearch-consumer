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

from datetime import datetime
import json
import spavro

from .config import get_consumer_config
from .jsonpath import CachedParser
from .logger import get_logger

LOG = get_logger('PROCESS')
CONSUMER_CONFIG = get_consumer_config()


ES_RESERVED = [
    '_uid', '_id', '_type', '_source', '_all', '_field_names',
    '_routing', '_index', '_size', '_timestamp', '_ttl', '_version'
]


class ESItemProcessor(object):

    def __init__(self, type_name, type_instructions):
        self.pipeline = []
        self.schema = None
        self.schema_obj = None
        self.es_type = type_name
        self.type_instructions = type_instructions
        self.topic_name = type_name
        self.has_parent = False

    def load_avro(self, schema_obj):
        self.schema = spavro.schema.parse(json.dumps(schema_obj))
        self.schema_obj = schema_obj
        self.load()

    def load(self):
        self.pipeline = []
        self.has_parent = False
        meta = self.type_instructions.get('_meta')
        if not meta:
            LOG.debug('type: %s has no meta arguments' % (self.es_type))
            return
        for key, value in meta.items():
            LOG.debug('Type %s has meta type: %s' % (self.es_type, key))
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
                    LOG.error('In finding geopoints in pipeline %s : %s' % (self.es_type, ver))
            elif key == 'aet_auto_ts':
                cmd = {
                    'function': '_add_timestamp',
                    'field_name': value
                }
                self.pipeline.append(cmd)
            elif key.startswith('aet'):
                LOG.debug('aet _meta keyword %s in type %s generates no command'
                          % (key, self.es_type))
            else:
                LOG.debug('Unknown meta keyword %s in type %s' % (key, self.es_type))
        LOG.debug('Pipeline for %s: %s' % (self.es_type, self.pipeline))

    def create_route(self):
        meta = self.type_instructions.get('_meta', {})
        join_field = meta.get('aet_join_field', None)
        if not self.has_parent or not join_field:
            LOG.debug('NO Routing created for type %s' % self.es_type)
            return lambda *args: None

        def route(doc):
            return doc.get(join_field).get('parent')
        LOG.debug('Routing created for child type %s' % self.es_type)
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
            LOG.error('Could not add parent to doc type %s. Error: %s' %
                      (self.es_type, e))
        return doc

    def _add_geopoint(self, doc, field_name=None, lat=None, lon=None, **kwargs):
        geo = {}
        try:
            geo['lat'] = float(self._get_doc_field(doc, lat))
            geo['lon'] = float(self._get_doc_field(doc, lon))
            doc[field_name] = geo
        except Exception as e:
            LOG.debug('Could not add geo to doc type %s. Error: %s | %s' %
                      (self.es_type, e, (lat, lon),))
        return doc

    def _add_timestamp(self, doc, field_name=None, **kwargs):
        doc[field_name] = str(datetime.now().isoformat())
        return doc

    def _get_doc_field(self, doc, name):
        if not name:
            raise ValueError('Invalid field name')
        doc = json.loads(json.dumps(doc))
        try:
            matches = CachedParser.find(name, doc)
            if not matches:
                raise ValueError(name, doc)
            if len(matches) > 1:
                LOG.warn('More than one value for %s in doc type %s, using first' %
                         (name, self.es_type))
            return matches[0].value
        except ValueError as ve:
            LOG.debug('Error getting field %s from doc type %s' %
                      (name, self.es_type))
            LOG.debug(doc)
            raise ve

    def _find_geopoints(self):
        res = {}
        latitude_fields = CONSUMER_CONFIG.get('latitude_fields')
        longitude_fields = CONSUMER_CONFIG.get('longitude_fields')
        LOG.debug(f'looking for matches in {latitude_fields} & {longitude_fields}')
        for lat in latitude_fields:
            path = self.find_path_in_schema(self.schema_obj, lat)
            if path:
                res['lat'] = path[0]  # Take the first Lat
                break
        for lng in longitude_fields:
            path = self.find_path_in_schema(self.schema_obj, lng)
            if path:
                res['lon'] = path[0]  # Take the first Lng
                break
        if 'lat' not in res or 'lon' not in res:
            raise ValueError('Could not resolve geopoints for field %s of type %s' % (
                'location', self.es_type))
        return res

    def find_path_in_schema(self, schema, test, previous_path='$'):
        # Searches a schema document for matching instances of an element.
        # Will look in nested objects. Aggregates matching paths.
        # LOG.debug(f'search: {test}:{previous_path}')
        matches = []
        if isinstance(schema, list):
            for _dict in schema:
                types = _dict.get('type')
                if not isinstance(types, list):
                    types = [types]  # treat everything as a union
                for _type in types:
                    if not _type:  # ignore nulls in unions
                        continue
                    if isinstance(_type, dict):
                        name = _dict.get('name')
                        next_level = _type.get('fields')
                        if next_level:
                            matches.extend(
                                self.find_path_in_schema(
                                    next_level,
                                    test,
                                    f'{previous_path}.{name}'
                                )
                            )
                    test_name = _dict.get('name', '').lower()
                    if str(test_name) == str(test.lower()):
                        return [f'{previous_path}.{test_name}']
        elif schema.get('fields'):
            matches.extend(
                self.find_path_in_schema(schema.get('fields'), test, previous_path)
            )
        return [m for m in matches if m]

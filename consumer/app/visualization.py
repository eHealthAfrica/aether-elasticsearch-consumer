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

from . import config
from . import index_handler
from .logger import get_logger
from .schema import Node

consumer_config = config.get_consumer_config()
LOG = get_logger('VIZ')


def get_map(title, alias, field_name):
    # MAP is a special case because the field name in the schema is incorrect
    # as we create a special geo_point type field and register it at index
    # creation time.

    # lookup actual field name
    geo_field_name = consumer_config.get(
        'autoconfig_settings').get(
        'geo_point_name', None
    )
    if not geo_field_name:
        raise ValueError('Geopoints not configured as part of autoconf')

    source_search = {
        'index': alias,
        'query': {
            'query': '',
            'language': 'lucene'
        },
        'filter': []
    }
    ui_state = {}
    vis_state = {
        'title': title,
        'type': 'tile_map',
        'params': {
            'colorSchema': 'Yellow to Red',
            'mapType': 'Scaled Circle Markers',
            'isDesaturated': True,
            'addTooltip': True,
            'heatClusterSize': 1.5,
            'legendPosition': 'bottomright',
            'mapZoom': 2,
            'mapCenter': [
                0,
                0
            ],
            'wms': {
                'enabled': False,
                'options': {
                    'format': 'image/png',
                    'transparent': True
                },
                'selectedTmsLayer': {
                    'maxZoom': 18,
                    'minZoom': 0,
                    'attribution': '',
                    'id': 'TMS in config/kibana.yml',
                    'origin': 'self_hosted'
                }
            }
        },
        'aggs': [
            {
                'id': '1',
                'enabled': True,
                'type': 'count',
                'schema': 'metric',
                'params': {}
            },
            {
                'id': '2',
                'enabled': True,
                'type': 'geohash_grid',
                'schema': 'segment',
                'params': {
                    'field': geo_field_name,
                    'autoPrecision': True,
                    'isFilteredByCollar': True,
                    'useGeocentroid': True,
                    'mapZoom': 2,
                    'mapCenter': [
                        0,
                        0
                    ],
                    'precision': 2
                }
            }
        ]
    }
    data = {
        'attributes': {
            'title': title,
            'uiStateJSON': json.dumps(ui_state, sort_keys=True),
            'visState': json.dumps(vis_state, sort_keys=True),
            'kibanaSavedObjectMeta': {
                'searchSourceJSON': json.dumps(source_search, sort_keys=True)
            }

        }
    }
    return data


VIS_MAP = {
    'geopoint': [
        ('TileMap', get_map)
    ]
}

VIS_ALIAS = {
    'geopoint': 'Map'
}


def _vis_for_type(_type: str):
    return VIS_MAP.get(_type, [])


def _supported_types():
    return [i for i in VIS_MAP.keys()]


def get_visualizations(
    alias: str,
    node: Node
):
    LOG.debug(f'Getting visualizations for {alias}')
    visualizations = {}
    for _type in _supported_types():
        handlers = _vis_for_type(_type)
        for vis_type, fn in handlers:
            paths = [i for i in node.find_children(
                {'match_attr': [{'__extended_type': _type}]}
            )]

            title_template = '{field_name}-{vis_type}'
            id_template = '{field_name}_{vis_type}'

            for path in paths:
                LOG.debug(f'matching path: {path}')
                field_name = index_handler.remove_formname(path)
                title = title_template.format(
                    field_name=field_name.capitalize(),
                    vis_type=vis_type.capitalize()
                )
                _id = id_template.format(
                    field_name=field_name.lower(),
                    vis_type=vis_type.lower()
                )
                res = fn(title, alias, field_name)
                visualizations[_id] = res
                LOG.debug(json.dumps([_id, res], indent=2))
    return visualizations

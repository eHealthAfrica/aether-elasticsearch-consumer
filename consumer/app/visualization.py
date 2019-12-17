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
from typing import List, Callable

from aet.logger import get_logger
from aether.python.avro.schema import Node

from . import config
from . import index_handler
from .processor import ES_RESERVED

consumer_config = config.get_consumer_config()
LOG = get_logger('VIZ')


AVRO_TYPES = [a_type for a_type, es_type in config.AVRO_TYPES]
AETHER_TYPES = [a_type for a_type, es_type in config.AETHER_TYPES]


def format_viz(fn):
    def do_format(*args, **kwargs):
        title = kwargs['title']
        alias = kwargs['alias']
        node = kwargs['node']
        field_name = kwargs['field_name']
        label = node.doc if hasattr(node, 'doc') else node.name
        vis_state = fn(title, alias, label, field_name, node)
        source_search = {
            'index': alias,
            'query': {
                'query': '',
                'language': 'lucene'
            },
            'filter': []
        }
        return {
            'attributes': {
                'title': title,
                'uiStateJSON': json.dumps({}),
                'visState': json.dumps(vis_state, sort_keys=True),
                'kibanaSavedObjectMeta': {
                    'searchSourceJSON': json.dumps(
                        source_search, sort_keys=True
                    )
                }

            }
        }
    return do_format


@format_viz
def get_map(
    title: str,
    alias: str,
    label: str,
    field_name: str,
    node: Node

):
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
    return vis_state


@format_viz
def get_table_histogram(  # numeric
    title: str,
    alias: str,
    label: str,
    field_name: str,
    node: Node

):
    vis_state = {
        'title': title,
        'type': 'table',
        'params': {
            'perPage': 10,
            'showPartialRows': False,
            'showMetricsAtAllLevels': False,
            'sort': {
                'columnIndex': None,
                'direction': None
            },
            'showTotal': False,
            'totalFunc': 'sum'
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
                'type': 'histogram',
                'schema': 'bucket',
                'params': {
                    'field': field_name,
                    'interval': 10,
                    'extended_bounds': {},
                    'customLabel': label
                }
            }
        ]
    }

    return vis_state


@format_viz
def get_pie_chart(
    title: str,
    alias: str,
    label: str,
    field_name: str,
    node: Node

):
    vis_state = {
        'title': title,
        'type': 'pie',
        'params': {
            'type': 'pie',
            'addTooltip': True,
            'addLegend': True,
            'legendPosition': 'right',
            'isDonut': True,
            'labels': {
                'show': False,
                'values': True,
                'last_level': True,
                'truncate': 100
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
                'type': 'terms',
                'schema': 'segment',
                'params': {
                    'field': field_name,
                    'size': 10,
                    'order': 'desc',
                    'orderBy': '_key',
                    'otherBucket': False,
                    'otherBucketLabel': 'Other',
                    'missingBucket': False,
                    'missingBucketLabel': 'Missing',
                    'customLabel': label
                }
            }
        ]
    }

    return vis_state


@format_viz
def get_table_buckets(  # text only
    title: str,
    alias: str,
    label: str,
    field_name: str,
    node: Node

):
    vis_state = {
        'title': title,
        'type': 'table',
        'params': {
            'perPage': 10,
            'showPartialRows': False,
            'showMetricsAtAllLevels': False,
            'sort': {
                'columnIndex': None,
                'direction': None
            },
            'showTotal': False,
            'totalFunc': 'sum'
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
                'type': 'terms',
                'schema': 'bucket',
                'params': {
                    'field': field_name,
                    'size': 5,
                    'order': 'desc',
                    'orderBy': '1',
                    'otherBucket': False,
                    'otherBucketLabel': 'Other',
                    'missingBucket': False,
                    'missingBucketLabel': 'Missing',
                    'customLabel': label
                }
            }
        ]
    }

    return vis_state


@format_viz
def get_barchart(  # numeric histogram / as bars
    title: str,
    alias: str,
    label: str,
    field_name: str,
    node: Node

):
    vis_state = {
        'title': title,
        'type': 'histogram',
        'params': {
            'type': 'histogram',
            'grid': {
                'categoryLines': False,
                'style': {
                    'color': '#eee'
                }
            },
            'categoryAxes': [
                {
                    'id': 'CategoryAxis-1',
                    'type': 'category',
                    'position': 'bottom',
                    'show': True,
                    'style': {},
                    'scale': {
                        'type': 'linear'
                    },
                    'labels': {
                        'show': True,
                        'truncate': 100
                    },
                    'title': {}
                }
            ],
            'valueAxes': [
                {
                    'id': 'ValueAxis-1',
                    'name': 'LeftAxis-1',
                    'type': 'value',
                    'position': 'left',
                    'show': True,
                    'style': {},
                    'scale': {
                        'type': 'linear',
                        'mode': 'normal'
                    },
                    'labels': {
                        'show': True,
                        'rotate': 0,
                        'filter': False,
                        'truncate': 100
                    },
                    'title': {
                        'text': 'Count'
                    }
                }
            ],
            'seriesParams': [
                {
                    'show': 'True',
                    'type': 'histogram',
                    'mode': 'stacked',
                    'data': {
                        'label': 'Count',
                        'id': '1'
                    },
                    'valueAxis': 'ValueAxis-1',
                    'drawLinesBetweenPoints': True,
                    'showCircles': True
                }
            ],
            'addTooltip': True,
            'addLegend': True,
            'legendPosition': 'right',
            'times': [],
            'addTimeMarker': False
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
                'type': 'histogram',
                'schema': 'segment',
                'params': {
                    'field': field_name,
                    'customLabel': label,
                    'interval': 5,
                    'extended_bounds': {}
                }
            }
        ]
    }

    return vis_state

# @format_viz
# def get_(
#     title: str,
#     alias: str,
#     label: str,
#     field_name: str,
#     node: Node

# ):
#     vis_state = {}
#     return vis_state


VIS_MAP = {
    'geopoint': [
        ('TileMap', get_map)
    ],
    'int': [
        ('Histogram', get_table_histogram),
        ('BarChart', get_barchart)
    ],
    'float': [
        ('Histogram', get_table_histogram),
        ('BarChart', get_barchart)
    ],
    'string': [
        ('PieChart', get_pie_chart),
        ('TableText', get_table_buckets)
    ],
    'select': [
        ('PieChart', get_pie_chart),
        ('TableText', get_table_buckets)
    ],
    'select1': [
        ('PieChart', get_pie_chart),
        ('TableText', get_table_buckets)
    ]
}


SCHEMA_VIS_MAP = {
    'histogram': ('Histogram', get_table_histogram),
    'pie': ('PieChart', get_pie_chart),
    'table': ('TableText', get_table_buckets),
    'map': ('TileMap', get_map)
}


def _vis_for_type(_type: str):
    return VIS_MAP.get(_type, [])


def _supported_types():
    return [i for i in VIS_MAP.keys()]


def __default_path_filters() -> List[Callable[[str], bool]]:
    # false -> omit

    def _filter_reserved(path):
        field_name = index_handler.remove_formname(path)
        if field_name in ES_RESERVED:
            return False
        return True

    def _filter_underscored(path):
        field_name = index_handler.remove_formname(path)
        if field_name[0] == '_':
            return False
        return True

    return [_filter_reserved, _filter_underscored]


def schema_defined_visualizations(
    alias: str,
    node: Node,
):
    visualizations = {}
    paths = [i for i in node.find_children(
        {'has_attr': ['__default_visualization']}
    )]
    LOG.debug(f'schemas found at paths {paths}')
    title_template = '{form_name} ({field_name} -> {vis_type})'
    id_template = '{form_name}_{field_name}_{vis_type}'
    for path in paths:
        target_node = node.get_node(path)
        vis_name = target_node.__default_visualization
        if vis_name not in SCHEMA_VIS_MAP:
            LOG.debug(f'@path: {path} has preferred type {vis_name}. No handler found')
            continue
        vis_type, fn = SCHEMA_VIS_MAP.get(vis_name)
        LOG.debug(f'visualizing path -> {path}')
        form_name = index_handler.get_formname(path)
        field_name = index_handler.remove_formname(path)
        title = title_template.format(
            form_name=form_name,
            field_name=field_name,
            vis_type=vis_type.capitalize()
        )
        _id = id_template.format(
            form_name=form_name.lower(),
            field_name=field_name.lower(),
            vis_type=vis_type.lower()
        )
        res = fn(
            title=title,
            alias=alias,
            field_name=field_name,
            node=target_node
        )
        visualizations[_id] = res
    return visualizations


def auto_visualizations(
    alias: str,
    node: Node,
    path_filters: List[Callable[[str], bool]] = __default_path_filters()
):
    LOG.debug(f'Getting visualizations for {alias}')
    visualizations = {}
    for _type in _supported_types():
        handlers = _vis_for_type(_type)
        for vis_type, fn in handlers:
            if _type in AETHER_TYPES:
                paths = [i for i in node.find_children(
                    {'match_attr': [{'__extended_type': _type}]}
                )]
            elif _type in AVRO_TYPES:
                paths = [i for i in node.find_children(
                    {'attr_contains': [{'avro_type': _type}]}
                )]

            title_template = '{form_name} ({field_name} -> {vis_type})'
            id_template = '{form_name}_{field_name}_{vis_type}'

            for path in paths:
                if path_filters and not all([fn(path) for fn in path_filters]):
                    LOG.debug(f'{path} ignored for visualization (filtered).')
                    continue
                LOG.debug(f'visualizing path -> {path}')
                form_name = index_handler.get_formname(path)
                field_name = index_handler.remove_formname(path)
                title = title_template.format(
                    form_name=form_name,
                    field_name=field_name,
                    vis_type=vis_type.capitalize()
                )
                _id = id_template.format(
                    form_name=form_name.lower(),
                    field_name=field_name.lower(),
                    vis_type=vis_type.lower()
                )
                res = fn(
                    title=title,
                    alias=alias,
                    field_name=field_name,
                    node=node.get_node(path)
                )
                visualizations[_id] = res
    return visualizations

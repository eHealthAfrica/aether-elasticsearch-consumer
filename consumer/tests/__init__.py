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
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import pytest
import os
from app.main import ESConsumerManager, connect_es, ESItemProcessor  # NOQA

# Some of the fixtures are non-compliant so we don't QA this file.
# flake8: noqa

class _MockConsumerManager(ESConsumerManager):

    def __init__(self, start_healthcheck=False):
        self.stopped = False
        self.autoconfigured_topics = []
        self.consumer_groups = {}
        if start_healthcheck:
            self.serve_healthcheck()


# We can use 'mark' distinctions to chose which tests are run and which assets are built
# @pytest.mark.integration
# @pytest.mark.unit
# When possible use fixtures for reusable test assets
# @pytest.fixture(scope='session')


@pytest.mark.integration
@pytest.fixture(scope='session')
def ElasticSearch():
    global es
    es = connect_es()
    return es


@pytest.mark.integration
@pytest.mark.unit
@pytest.fixture(scope='function')
def MockConsumerManager():
    return _MockConsumerManager


@pytest.mark.integration
@pytest.fixture(scope='function')
def ConsumerManager(ElasticSearch):
    return ESConsumerManager


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def AutoConfigSettings():
    path = os.environ['ES_CONSUMER_CONFIG_PATH']
    with open(path) as f:
        obj = json.load(f)
        return obj.get('autoconfig_settings')


SAMPLE_DOC = {
    'start': '2018-08-14T13:50:04.064000+01:00',
    'end': '2018-08-14T13:52:51.024000+01:00',
    'today': '2018-08-14T00:00:00',
    'deviceid': '355662090560127',
    'phonenumber': None,
    'note_start': None,
    'acknowledge_intro': 'OK',
    'residents_module': {
        'supervisor_name': 'patricia_gauji',
        'enumerator': 'idris_muazu',
        'cluster': 'constitution_independence',
        'respondent_name': 'Rukayyah',
        'gender': 'female',
        'age': 41,
        'connected_discos_supplied': 'no',
        'use_alternative_power': 'yes',
        'largest_source_noise': 'generator musica trade traffic',
        'wish_to_relocate': 'yes',
        'why_would_you_choose_to_relocat': 'noise power_supply quality_of_light safety traffic',
        'common_safety_concerns_in_the_market': 'vandalism robbery_personal',
        'comments': 'None'
    },
    'pic': '1534251107462.jpg',
    'geo': {
        'latitude': 10.513823509216309,
        'longitude': 7.4525299072265625,
        'altitude': 598.2000122070312,
        'accuracy': 4.699999809265137
    },
    'meta': {
        'instanceID': 'uuid:601e659e-7393-450b-a59d-f80706e7f55d'
    },
    'id': '2856a498-1705-4d2f-b083-ba4199619705',
    '_id': 'residence_questionnaire',
    '_version': '1'
}

SAMPLE_DOC2 = {
        'Encounter_Date_Time': None,
        'Location': {
            'accuracy': 26.0,
            'altitude': 486.0,
            'latitude': 9.070648346096277,
            'longitude': 7.413686318323016
        },
        'Patient_Age': 29,
        'Patient_Name': 'Jane Smith ',
        'QR_Code': '0626b3a2-401c-4012-8b81-1f5b14df8c7b',
        'Test_Name': 'TEST_1',
        '_id': 'rapidtest_start',
        '_version': '0',
        'end': '2019-01-28T09:05:51.154000+01:00',
        'id': '75dc93fa-647a-4c53-bc5a-18aa5394fd40',
        'meta': {
            'instanceID': 'uuid:adda71c0-2099-4123-87d1-c210838e0565'
        },
        'start': '2019-01-28T09:05:18.680000+01:00'
    }

TYPE_INSTRUCTIONS = {
    '_meta': {
        'aet_subscribed_topics': [
            'Residence_Questionnaire_1_3'
        ],
        'aet_geopoint': 'geo_point'
    }
}

DOC_SCHEMA = {
    'type': 'record',
    'fields': [
        {
            'name': 'start',
            'type': 'string'
        },
        {
            'name': 'end',
            'type': 'string'
        },
        {
            'name': 'today',
            'type': 'string'
        },
        {
            'name': 'deviceid',
            'type': 'string'
        },
        {
            'name': 'phonenumber',
            'type': 'None'
        },
        {
            'name': 'note_start',
            'type': 'None'
        },
        {
            'name': 'acknowledge_intro',
            'type': 'string'
        },
        {
            'name': 'residents_module',
            'type': {
                'type': 'record',
                'fields': [
                    {
                        'name': 'supervisor_name',
                        'type': 'string'
                    },
                    {
                        'name': 'enumerator',
                        'type': 'string'
                    },
                    {
                        'name': 'cluster',
                        'type': 'string'
                    },
                    {
                        'name': 'respondent_name',
                        'type': 'string'
                    },
                    {
                        'name': 'gender',
                        'type': 'string'
                    },
                    {
                        'name': 'age',
                        'type': 'int'
                    },
                    {
                        'name': 'connected_discos_supplied',
                        'type': 'string'
                    },
                    {
                        'name': 'use_alternative_power',
                        'type': 'string'
                    },
                    {
                        'name': 'largest_source_noise',
                        'type': 'string'
                    },
                    {
                        'name': 'wish_to_relocate',
                        'type': 'string'
                    },
                    {
                        'name': 'why_would_you_choose_to_relocat',
                        'type': 'string'
                    },
                    {
                        'name': 'common_safety_concerns_in_the_market',
                        'type': 'string'
                    },
                    {
                        'name': 'comments',
                        'type': 'string'
                    }
                ],
                'name': 'Auto_1'
            }
        },
        {
            'name': 'pic',
            'type': 'string'
        },
        {
            'name': 'geo',
            'type': {
                'type': 'record',
                'fields': [
                    {
                        'name': 'latitude',
                        'type': 'float'
                    },
                    {
                        'name': 'longitude',
                        'type': 'float'
                    },
                    {
                        'name': 'altitude',
                        'type': 'float'
                    },
                    {
                        'name': 'accuracy',
                        'type': 'float'
                    }
                ],
                'name': 'Auto_2'
            }
        },
        {
            'name': 'meta',
            'type': {
                'type': 'record',
                'fields': [
                    {
                        'name': 'instanceID',
                        'type': 'string'
                    }
                ],
                'name': 'Auto_3'
            }
        },
        {
            'name': 'id',
            'type': 'string'
        },
        {
            'name': 'es_reserved__id',
            'type': 'string'
        },
        {
            'name': 'es_reserved__version',
            'type': 'string'
        }
    ],
    'name': 'Auto_0'
}


DOC_SCHEMA2 = {
    'doc': 'Rapid Test - Start (id: rapidtest_start, version: 2019012807)',
    'fields': [
        {
            'default': 'rapidtest_start',
            'doc': 'xForm ID',
            'name': '_id',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'default': '2019012807',
            'doc': 'xForm version',
            'name': '_version',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'name': 'start',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'name': 'end',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'doc': 'Test Name',
            'name': 'Test_Name',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'doc': 'Scan QR Code',
            'name': 'QR_Code',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'doc': 'Patient Name',
            'name': 'Patient_Name',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'doc': 'Patient Age',
            'name': 'Patient_Age',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'int'
            ]
        },
        {
            'doc': 'Location',
            'name': 'Location',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                {
                    'doc': 'Location',
                    'fields': [
                        {
                            'doc': 'latitude',
                            'name': 'latitude',
                            'namespace': 'Rapidtest_Start_2019012807.Location',
                            'type': [
                                'None',
                                'float'
                            ]
                        },
                        {
                            'doc': 'longitude',
                            'name': 'longitude',
                            'namespace': 'Rapidtest_Start_2019012807.Location',
                            'type': [
                                'None',
                                'float'
                            ]
                        },
                        {
                            'doc': 'altitude',
                            'name': 'altitude',
                            'namespace': 'Rapidtest_Start_2019012807.Location',
                            'type': [
                                'None',
                                'float'
                            ]
                        },
                        {
                            'doc': 'accuracy',
                            'name': 'accuracy',
                            'namespace': 'Rapidtest_Start_2019012807.Location',
                            'type': [
                                'None',
                                'float'
                            ]
                        }
                    ],
                    'name': 'Location',
                    'namespace': 'Rapidtest_Start_2019012807',
                    'type': 'record'
                }
            ]
        },
        {
            'name': 'Encounter_Date_Time',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                'string'
            ]
        },
        {
            'name': 'meta',
            'namespace': 'Rapidtest_Start_2019012807',
            'type': [
                'None',
                {
                    'fields': [
                        {
                            'name': 'instanceID',
                            'namespace': 'Rapidtest_Start_2019012807.meta',
                            'type': [
                                'None',
                                'string'
                            ]
                        }
                    ],
                    'name': 'meta',
                    'namespace': 'Rapidtest_Start_2019012807',
                    'type': 'record'
                }
            ]
        },
        {
            'doc': 'UUID',
            'name': 'id',
            'type': 'string'
        }
    ]
}

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


SAMPLE_FIELD_LOOKUP = {
    'occupant_gender': {
        'id': 'static_lookup',
        'params': {
            'lookupEntries': [
                {
                    'value': 'Operational',
                    'key': 'operational'
                },
                {
                    'value': 'Non Operational',
                    'key': 'non_operational'
                },
                {
                    'value': 'Unknown',
                    'key': 'unknown'
                }
            ],
            'unknownKeyValue': 'N/A'
        }
    }
}

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


ANNOTATED_SCHEMA = {
    'doc': 'HS OSM Gather Test (id: gth_hs_test, version: 2)',
    'name': 'Gth_Hs_Test_2',
    'type': 'record',
    'fields': [
        {
            'doc': 'xForm ID',
            'name': '_id',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2'
        },
        {
            'doc': 'xForm version',
            'name': '_version',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2'
        },
        {
            'name': '_start',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'dateTime'
        },
        {
            'name': 'timestamp',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'dateTime'
        },
        {
            'name': 'username',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'source',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'osm_id',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Name of Facility',
            'name': 'name',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Address',
            'name': 'addr_full',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Phone Number',
            'name': 'contact_number',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Facility Operator Name',
            'name': 'operator',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Operator Type',
            'name': 'operator_type',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Public',
                    'value': 'public'
                },
                {
                    'label': 'Private',
                    'value': 'private'
                },
                {
                    'label': 'Community',
                    'value': 'community'
                },
                {
                    'label': 'Religious',
                    'value': 'religious'
                },
                {
                    'label': 'Government',
                    'value': 'government'
                },
                {
                    'label': 'NGO',
                    'value': 'ngo'
                },
                {
                    'label': 'Combination',
                    'value': 'combination'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Facility Location',
            'name': 'geometry',
            'type': [
                'null',
                {
                    'doc': 'Facility Location',
                    'name': 'geometry',
                    'type': 'record',
                    'fields': [
                        {
                            'doc': 'latitude',
                            'name': 'latitude',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'Gth_Hs_Test_2.geometry'
                        },
                        {
                            'doc': 'longitude',
                            'name': 'longitude',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'Gth_Hs_Test_2.geometry'
                        },
                        {
                            'doc': 'altitude',
                            'name': 'altitude',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'Gth_Hs_Test_2.geometry'
                        },
                        {
                            'doc': 'accuracy',
                            'name': 'accuracy',
                            'type': [
                                'null',
                                'float'
                            ],
                            'namespace': 'Gth_Hs_Test_2.geometry'
                        }
                    ],
                    'namespace': 'Gth_Hs_Test_2',
                    '@aether_extended_type': 'geopoint'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'geopoint'
        },
        {
            'doc': 'Operational Status',
            'name': 'operational_status',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Operational',
                    'value': 'operational'
                },
                {
                    'label': 'Non Operational',
                    'value': 'non_operational'
                },
                {
                    'label': 'Unknown',
                    'value': 'unknown'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'When is the facility open?',
            'name': '_opening_hours_type',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Pick the days of the week open and enter hours for each day',
                    'value': 'oh_select'
                },
                {
                    'label': 'Only open on weekdays with the same hours every day.',
                    'value': 'oh_weekday'
                },
                {
                    'label': '24/7 - All day, every day',
                    'value': 'oh_24_7'
                },
                {
                    'label': 'Type in OSM String by hand (Advanced Option)',
                    'value': 'oh_advanced'
                },
                {
                    'label': '''I don't know the operating hours''',
                    'value': 'oh_unknown'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Which days is this facility open?',
            'name': '_open_days',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Monday',
                    'value': 'Mo'
                },
                {
                    'label': 'Tuesday',
                    'value': 'Tu'
                },
                {
                    'label': 'Wednesday',
                    'value': 'We'
                },
                {
                    'label': 'Thursday',
                    'value': 'Th'
                },
                {
                    'label': 'Friday',
                    'value': 'Fr'
                },
                {
                    'label': 'Saturday',
                    'value': 'Sa'
                },
                {
                    'label': 'Sunday',
                    'value': 'Su'
                },
                {
                    'label': 'Public Holidays',
                    'value': 'PH'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Open hours by day of the week',
            'name': '_dow_group',
            'type': [
                'null',
                {
                    'doc': 'Open hours by day of the week',
                    'name': '_dow_group',
                    'type': 'record',
                    'fields': [
                        {
                            'doc': 'Enter open hours for each day:',
                            'name': '_hours_note',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Monday open hours',
                            'name': '_mon_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Tuesday open hours',
                            'name': '_tue_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Wednesday open hours',
                            'name': '_wed_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Thursday open hours',
                            'name': '_thu_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Friday open hours',
                            'name': '_fri_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Saturday open hours',
                            'name': '_sat_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Sunday open hours',
                            'name': '_sun_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'doc': 'Public Holiday open hours',
                            'name': '_ph_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        },
                        {
                            'name': '_select_hours',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2._dow_group',
                            '@aether_extended_type': 'string'
                        }
                    ],
                    'namespace': 'Gth_Hs_Test_2',
                    '@aether_extended_type': 'group'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'group'
        },
        {
            'doc': 'Enter weekday hours',
            'name': '_weekday_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': '''OSM 'opening_hours ''',
            'name': '_advanced_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'opening_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Verify the open hours are correct or go back and fix:',
            'name': '_disp_hours',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'Facility Category',
            'name': 'amenity',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Clinic',
                    'value': 'clinic'
                },
                {
                    'label': 'Doctors',
                    'value': 'doctors'
                },
                {
                    'label': 'Hospital',
                    'value': 'hospital'
                },
                {
                    'label': 'Dentist',
                    'value': 'dentist'
                },
                {
                    'label': 'Pharmacy',
                    'value': 'pharmacy'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Available Services',
            'name': 'healthcare',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Doctor',
                    'value': 'doctor'
                },
                {
                    'label': 'Pharmacy',
                    'value': 'pharmacy'
                },
                {
                    'label': 'Hospital',
                    'value': 'hospital'
                },
                {
                    'label': 'Clinic',
                    'value': 'clinic'
                },
                {
                    'label': 'Dentist',
                    'value': 'dentist'
                },
                {
                    'label': 'Physiotherapist',
                    'value': 'physiotherapist'
                },
                {
                    'label': 'Alternative',
                    'value': 'alternative'
                },
                {
                    'label': 'Laboratory',
                    'value': 'laboratory'
                },
                {
                    'label': 'Optometrist',
                    'value': 'optometrist'
                },
                {
                    'label': 'Rehabilitation',
                    'value': 'rehabilitation'
                },
                {
                    'label': 'Blood donation',
                    'value': 'blood_donation'
                },
                {
                    'label': 'Birthing center',
                    'value': 'birthing_center'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Specialities',
            'name': 'speciality',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'xx',
                    'value': 'xx'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Speciality medical equipment available',
            'name': 'health_amenity_type',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Ultrasound',
                    'value': 'ultrasound'
                },
                {
                    'label': 'MRI',
                    'value': 'mri'
                },
                {
                    'label': 'X-Ray',
                    'value': 'x_ray'
                },
                {
                    'label': 'Dialysis',
                    'value': 'dialysis'
                },
                {
                    'label': 'Operating Theater',
                    'value': 'operating_theater'
                },
                {
                    'label': 'Laboratory',
                    'value': 'laboratory'
                },
                {
                    'label': 'Imaging Equipment',
                    'value': 'imaging_equipment'
                },
                {
                    'label': 'Intensive Care Unit',
                    'value': 'intensive_care_unit'
                },
                {
                    'label': 'Emergency Department',
                    'value': 'emergency_department'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Does this facility provide Emergency Services?',
            'name': 'emergency',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Yes',
                    'value': 'yes'
                },
                {
                    'label': 'No',
                    'value': 'no'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Does the pharmacy dispense prescription medication?',
            'name': 'dispensing',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Yes',
                    'value': 'yes'
                },
                {
                    'label': 'No',
                    'value': 'no'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'Number of Beds',
            'name': 'beds',
            'type': [
                'null',
                'int'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'int'
        },
        {
            'doc': 'Number of Doctors',
            'name': 'staff_doctors',
            'type': [
                'null',
                'int'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'int'
        },
        {
            'doc': 'Number of Nurses',
            'name': 'staff_nurses',
            'type': [
                'null',
                'int'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'int'
        },
        {
            'doc': 'Types of insurance accepted?',
            'name': 'insurance',
            'type': [
                'null',
                {
                    'type': 'array',
                    'items': 'string'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Public',
                    'value': 'public'
                },
                {
                    'label': 'Private',
                    'value': 'private'
                },
                {
                    'label': 'None',
                    'value': 'no'
                },
                {
                    'label': 'Unknown',
                    'value': 'unknown'
                }
            ],
            '@aether_extended_type': 'select'
        },
        {
            'doc': 'Is this facility wheelchair accessible?',
            'name': 'wheelchair',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Yes',
                    'value': 'yes'
                },
                {
                    'label': 'No',
                    'value': 'no'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'What is the source of water for this facility?',
            'name': 'water_source',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Well',
                    'value': 'well'
                },
                {
                    'label': 'Water works',
                    'value': 'water_works'
                },
                {
                    'label': 'Manual pump',
                    'value': 'manual_pump'
                },
                {
                    'label': 'Powered pump',
                    'value': 'powered_pump'
                },
                {
                    'label': 'Groundwater',
                    'value': 'groundwater'
                },
                {
                    'label': 'Rain',
                    'value': 'rain'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'What is the source of power for this facility?',
            'name': 'electricity',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_lookup': [
                {
                    'label': 'Power grid',
                    'value': 'grid'
                },
                {
                    'label': 'Generator',
                    'value': 'generator'
                },
                {
                    'label': 'Solar',
                    'value': 'solar'
                },
                {
                    'label': 'Other Power',
                    'value': 'other'
                },
                {
                    'label': 'No Power',
                    'value': 'none'
                }
            ],
            '@aether_extended_type': 'select1'
        },
        {
            'doc': 'URL for this location (if available)',
            'name': 'url',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'In which health are is the facility located?',
            'name': 'is_in_health_area',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'doc': 'In which health zone is the facility located?',
            'name': 'is_in_health_zone',
            'type': [
                'null',
                'string'
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'string'
        },
        {
            'name': 'meta',
            'type': [
                'null',
                {
                    'name': 'meta',
                    'type': 'record',
                    'fields': [
                        {
                            'name': 'instanceID',
                            'type': [
                                'null',
                                'string'
                            ],
                            'namespace': 'Gth_Hs_Test_2.meta',
                            '@aether_extended_type': 'string'
                        }
                    ],
                    'namespace': 'Gth_Hs_Test_2',
                    '@aether_extended_type': 'group'
                }
            ],
            'namespace': 'Gth_Hs_Test_2',
            '@aether_extended_type': 'group'
        },
        {
            'doc': 'UUID',
            'name': 'id',
            'type': 'string'
        }
    ],
    'namespace': 'org.ehealthafrica.aether.odk.xforms.Hsdemo'
}

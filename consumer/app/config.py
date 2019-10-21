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

import os

from aet.settings import Settings

consumer_config = None
kafka_config = None


# Mappings types to ES equivalents
AVRO_TYPES = [
    ('boolean', 'boolean'),
    ('int', 'integer'),
    ('long', 'long'),
    ('float', 'float'),
    ('double', 'double'),
    ('bytes', 'binary'),
    ('string', 'keyword'),
    ('record', 'object'),
    ('enum', 'string'),
    ('array', 'nested'),
    ('fixed', 'string'),
    ('object', 'object'),
    ('array:string', 'object')
]

AETHER_TYPES = [
    ('dateTime', 'date'),
    ('geopoint', 'object'),  # our geopoints don't always fit ES requirements
    ('select', 'keyword'),
    ('select1', 'keyword'),
    ('group', 'object')
]


def load_config():
    CONSUMER_CONFIG_PATH = os.environ['ES_CONSUMER_CONFIG_PATH']
    KAFKA_CONFIG_PATH = os.environ['ES_CONSUMER_KAFKA_CONFIG_PATH']
    global consumer_config
    consumer_config = Settings(file_path=CONSUMER_CONFIG_PATH)
    global kafka_config
    kafka_config = Settings(
        file_path=KAFKA_CONFIG_PATH,
        alias={'bootstrap.servers': 'kafka_url'},
        exclude=['kafka_url']
    )


def get_kafka_config():
    # load security settings in from environment
    # if the security protocol is set
    if kafka_config.get('security.protocol'):
        for i in [
            'security.protocol',
            'sasl.mechanism',
            'sasl.username',
            'sasl.password'
        ]:
            kafka_config[i] = kafka_config.get(i)
    return kafka_config


def get_consumer_config():
    return consumer_config


load_config()

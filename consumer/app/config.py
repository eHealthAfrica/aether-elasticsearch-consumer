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

kafka_admin_uses = [
    'bootstrap.servers',
    'security.protocol',
    'sasl.mechanism',
    'sasl.username',
    'sasl.password'
]

# Mappings types to ES equivalents
AVRO_TYPES = [
    # avro -> type, [optional] format
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
    ('array:string', 'object'),
]

AVRO_LOGICAL_TYPES = [
    # avro logical types
    # # requires conversion (days since epoch [int] -> seconds)
    ('date', ('date', 'date')),
    ('timestamp-millis', ('date', 'epoch_millis')),
    ('uuid', 'string'),
]

AETHER_TYPES = [
    ('dateTime', 'date'),
    ('epoch_second', ('date', 'epoch_second')),
    ('epoch_millis', ('date', 'epoch_millis')),
    ('geopoint', 'object'),  # our geopoints don't always fit ES requirements
    ('select', 'keyword'),
    ('select1', 'keyword'),
    ('group', 'object')
]


def load_config():
    CONSUMER_CONFIG_PATH = os.environ.get('ES_CONSUMER_CONFIG_PATH', None)
    KAFKA_CONFIG_PATH = os.environ.get('ES_CONSUMER_KAFKA_CONFIG_PATH', None)
    global consumer_config
    consumer_config = Settings(file_path=CONSUMER_CONFIG_PATH)
    global kafka_config
    kafka_config = Settings(
        file_path=KAFKA_CONFIG_PATH,
        alias={'BOOTSTRAP.SERVERS': 'KAFKA_URL'},
        exclude=['KAFKA_URL']
    )


def get_kafka_config():
    # load security settings in from environment
    # if the security protocol is set
    protocol = kafka_config.get('SECURITY.PROTOCOL') or \
        kafka_config.get('KAFKA_CONSUMER_SECURITY_PROTOCOL')
    if protocol:
        for name, alias in [
            ('SECURITY.PROTOCOL', 'KAFKA_CONSUMER_SECURITY_PROTOCOL'),
            ('SASL.MECHANISM', 'KAFKA_CONSUMER_SASL_MECHANISM'),
            ('SASL.USERNAME', 'KAFKA_CONSUMER_USER'),
            ('SASL.PASSWORD', 'KAFKA_CONSUMER_PASSWORD')
        ]:
            if not kafka_config.get(name):
                kafka_config[name] = kafka_config.get(alias)
            else:
                kafka_config[name] = kafka_config.get(name)
    return kafka_config


def get_kafka_admin_config():
    kafka_security = get_kafka_config().copy()
    ks_keys = list(kafka_security.keys())
    for i in ks_keys:
        if i.lower() not in kafka_admin_uses:
            del kafka_security[i]
        else:
            kafka_security[i.lower()] = kafka_security[i]
            del kafka_security[i]
    return kafka_security


def get_consumer_config():
    return consumer_config


load_config()

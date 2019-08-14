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

import json
import os

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


class Settings(dict):
    # A container for our settings
    def __init__(self, file_path=None, alias=None, exclude=None):
        if not exclude:
            self.exclude = []
        else:
            self.exclude = exclude
        self.alias = alias
        self.load(file_path)

    def get(self, key, default=None):
        if self.exclude is not None and key in self.exclude:
            return default
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key):
        if self.alias and key in self.alias:
            key = self.alias.get(key)
        result = os.environ.get(key.upper())
        if result is None:
            result = super().__getitem__(key)

        return result

    def copy(self):
        keys = [k for k in self.keys() if k not in self.exclude]
        for key in self.alias:
            keys.append(key)
        return {k: self.get(k) for k in keys}

    def load(self, path):
        with open(path) as f:
            obj = json.load(f)
            for k in obj:
                self[k] = obj.get(k)


def load_config():
    CONSUMER_CONFIG_PATH = os.environ['ES_CONSUMER_CONFIG_PATH']
    KAFKA_CONFIG_PATH = os.environ['ES_CONSUMER_KAFKA_CONFIG_PATH']
    global consumer_config
    consumer_config = Settings(file_path=CONSUMER_CONFIG_PATH)
    global kafka_config
    kafka_config = Settings(
        file_path=KAFKA_CONFIG_PATH,
        alias={'bootstrap_servers': 'kafka_url'},
        exclude=['kafka_url']
    )


def get_kafka_config():
    # load security settings in from environment
    # if the security protocol is set
    if kafka_config.get('security_protocol'):
        for i in [
            'security_protocol',
            'sasl_mechanism',
            'sasl_plain_username',
            'sasl_plain_password'
        ]:
            kafka_config[i] = kafka_config.get(i)
    return kafka_config


def get_consumer_config():
    return consumer_config


load_config()

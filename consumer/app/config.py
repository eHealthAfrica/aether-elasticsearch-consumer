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


class Settings(dict):
    # A container for our settings
    def __init__(self, file_path=None):
        self.load(file_path)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key):
        result = os.environ.get(key.upper())
        if result is None:
            result = super().__getitem__(key)

        return result

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
    kafka_config = Settings(file_path=KAFKA_CONFIG_PATH)


def get_kafka_config():
    return kafka_config


def get_consumer_config():
    return consumer_config


load_config()

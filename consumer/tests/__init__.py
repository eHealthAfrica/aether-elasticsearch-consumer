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
import pytest
import os
from app.main import ESConsumerManager

kafka_server = 'kafka-test:29099'


# We can use 'mark' distinctions to chose which tests are run and which assets are built
# @pytest.mark.integration
# @pytest.mark.unit
# When possible use fixtures for reusable test assets
# @pytest.fixture(scope='session')

@pytest.mark.integration
@pytest.mark.unit
@pytest.fixture(scope='function')
def MockConsumerManager():
    return ESConsumerManager()


@pytest.mark.unit
@pytest.mark.integration
@pytest.fixture(scope='session')
def AutoConfigSettings():
    path = os.environ['ES_CONSUMER_CONFIG_PATH']
    with open(path) as f:
        obj = json.load(f)
        return obj.get('autoconfig_settings')

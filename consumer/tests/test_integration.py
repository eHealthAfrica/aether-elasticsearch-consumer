#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

from . import *  # get all test assets from test/__init__.py
from time import sleep

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`
# to run integration tests / all tests run the test_all.sh script from the /tests directory.


@pytest.mark.integration
def test_consumer_manager__get_indexes_for_auto_config(MockConsumerManager, AutoConfigSettings):
    res = MockConsumerManager.get_indexes_for_auto_config(**AutoConfigSettings)
    assert(len(res) == 4), 'There should be 4 available indexes in this set.'


@pytest.mark.integration
def test_consumer_manager__init(ConsumerManager, ElasticSearch):
    try:
        # wait for connection to ES
        sleep(5)
        groups = [name for name in ConsumerManager.consumer_groups]
        assert(len(groups) is 5)  # one each for 4 autoconfig, 1 for combined.json
    except Exception as err:
        raise(err)
    finally:
        ConsumerManager.stop()

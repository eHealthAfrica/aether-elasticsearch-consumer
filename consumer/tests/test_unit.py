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

import os

from . import *  # noqa # get all test assets from test/__init__.py
from app.main import KAFKA_CONFIG

# Test Suite contains both unit and integration tests
# Unit tests can be run on their own from the root directory
# enter the bash environment for the version of python you want to test
# for example for python 3
# `docker-compose run consumer-sdk-test bash`
# then start the unit tests with
# `pytest -m unit`
# to run integration tests / all tests run the test_all.sh script from the /tests directory.


@pytest.mark.unit
def test__get_config_alias():
    assert(KAFKA_CONFIG.get('bootstrap_servers') is not None)
    args = KAFKA_CONFIG.copy()
    assert('bootstrap_servers' in args)
    assert(args.get('bootstrap_servers') == os.environ.get('KAFKA_URL'))
    assert(args.get('kafka_url') is None)
    assert(KAFKA_CONFIG.get('kafka_url') is None)


@pytest.mark.unit
def test__get_field_by_name():
    processor = ESItemProcessor(None, None)
    vals = [
        ('today', SAMPLE_DOC.get('today')),
        ('residents_module', SAMPLE_DOC.get('residents_module')),
        ('residents_module.supervisor_name', SAMPLE_DOC.get(
            'residents_module').get('supervisor_name')),
        ('geo.latitude', SAMPLE_DOC.get('geo').get('latitude'))
    ]
    for field, value in vals:
        assert(processor._get_doc_field(SAMPLE_DOC, field) == value)


@pytest.mark.unit
def test__process_geo_field():
    to_test = [
        [TYPE_INSTRUCTIONS, DOC_SCHEMA, SAMPLE_DOC],
        [TYPE_INSTRUCTIONS, DOC_SCHEMA2, SAMPLE_DOC2]
    ]
    for instr, schema, doc in to_test:
        processor = ESItemProcessor('test', instr)
        processor.schema_obj = schema
        processor.load()
        res = processor._find_geopoints()
        assert(res.get('lat') is not None)
        doc = processor.process(doc)
        assert(doc.get('geo_point').get('lon') is not None)

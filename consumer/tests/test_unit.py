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
from app import utils

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
        [TYPE_INSTRUCTIONS, AUTOGEN_SCHEMA, SAMPLE_DOC],
        [TYPE_INSTRUCTIONS, SIMPLE_SCHEMA, SAMPLE_DOC2]
    ]
    for instr, schema, doc in to_test:
        processor = ESItemProcessor('test', instr)
        processor.schema_obj = schema
        processor.load()
        res = processor._find_geopoints()
        assert(res.get('lat') is not None)
        doc = processor.process(doc)
        assert(doc.get('geo_point').get('lon') is not None)


@pytest.mark.unit
def test__hash():
    pairs = [
        ('a', 'n', False),
        ({
            'b': ['a', 'b'],
            'a': ['a', 'b']},  # swap order of keys
            {
            'a': ['a', 'b'],
            'b': ['a', 'b']},
            True),
        (1, 2, False),
        ('a', 'a', True),
        (2, 2, True),
    ]

    for a, b, match in pairs:
        assert((utils.hash(a) == utils.hash(b)) == match), [a, b, match]


@pytest.mark.unit
def test__merge_dicts():
    cases = [
        (
            {'a': [1, 2, 3]},
            {'b': [4, 5, 6]},
            {'a': [1, 2, 3], 'b': [4, 5, 6]}
        ),
        (
            {'a': [1, 2, 3]},
            {'a': [4, 5, 6]},
            {'a': [1, 2, 3, 4, 5, 6]}
        ),
        (
            {'a': {'a': 1, 'b': 0, 'c': 0}, 'b': {'a': 0, 'b': 2, 'c': 3}},
            {'a': {'b': 2, 'c': 3}, 'b': {'b': 0, 'c': 0}},
            {'a': {'a': 1, 'b': 2, 'c': 3}, 'b': {'a': 0, 'b': 0, 'c': 0}}
        )
    ]
    for a, b, c in cases:
        assert(utils.merge_dicts(a, b) == c)

# Copyright (C) 2019 by eHealth Africa : http://www.eHealthAfrica.org
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

import pytest

from aet.logger import get_logger


from app import processor

from . import *  # noqa  # fixtures
from . import (
    TYPE_INSTRUCTIONS
)


LOG = get_logger('TEST-PRO')


@pytest.mark.parametrize('_type,test_value,expected', [
    ('date', 1, '1970-01-02'),
])
@pytest.mark.unit
def test__required_avro_logical_coersions(_type, test_value, expected):
    _fn = processor.AVRO_LOGICAL_COERSCE[_type]
    res = _fn(test_value)
    assert(res == expected)


@pytest.mark.unit
def test__end_to_end(ComplexSchema):
    _name = 'test'
    _instr = TYPE_INSTRUCTIONS
    proc = processor.ESItemProcessor(_name, _instr, ComplexSchema)
    # proc.load_avro(_schema)
    doc = {'geometry': {
        'latitude': 1.0, 'longitude': 1.0}, 'mandatory_date': 10957, 'optional_dt': 1595431696000}
    res = proc.process(doc)
    LOG.debug(res)
    assert(res['geo_point']['lat'] == res['geo_point']['lon'])
    assert(isinstance(res['a_ts'], str))
    assert(isinstance(res['mandatory_date'], str))
    assert(res['mandatory_date'] == '2000-01-01')
    assert(res['optional_dt'] == 1595431696000)


@pytest.mark.unit
def test__most_permissive_avro_type():
    _test = ['object', 'null', 'string', 'not-real']
    res = processor.ESItemProcessor._most_permissive_avro_type(_test)
    assert(res == 'object')

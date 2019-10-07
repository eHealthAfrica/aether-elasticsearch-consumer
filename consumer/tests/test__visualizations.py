#!/usr/bin/env python

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

import json
import pytest

from app.logger import get_logger
from app.visualization import (
    auto_visualizations,
    schema_defined_visualizations
)

from . import *  # noqa  # fixtures

LOG = get_logger('TEST-VIZ')


@pytest.mark.unit
def test__get_auto_visualizations(ComplexSchema):
    res = auto_visualizations('com.example', ComplexSchema)
    LOG.debug(json.dumps(res, indent=2))
    assert(sum([1 for k in res.keys()]) == 67)


@pytest.mark.unit
def test__get_schema_visualizations(ComplexSchema):
    res = schema_defined_visualizations('com.example', ComplexSchema)
    LOG.debug(json.dumps(res, indent=2))
    assert(sum([1 for k in res.keys()]) == 2)

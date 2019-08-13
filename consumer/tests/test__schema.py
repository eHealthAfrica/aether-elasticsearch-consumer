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

from copy import deepcopy
import pytest

from app.logger import get_logger
LOG = get_logger('TEST-SCH')

from . import *  # noqa  # fixtures


@pytest.mark.unit
def test__comparison_none(SimpleSchema):
    assert(Node.compare(SimpleSchema, SimpleSchema) == {})


@pytest.mark.unit
def test__comparison_all(SimpleSchema):
    a = deepcopy(SimpleSchema)
    b = deepcopy(SimpleSchema)
    a.name = 'SomethingElse'  # change root node (changes all paths)
    assert(len(Node.compare(a, b)) == 15)  # all nodes


@pytest.mark.unit
def test__comparison_nested_attr(ComplexSchema):
    a = deepcopy(ComplexSchema)
    b = deepcopy(ComplexSchema)
    path = 'operator_type'
    # change a node's attribute
    a.children[path].__lookup = [{"something": "else"}]
    # I don't always trust deepcopy...
    assert(a.children[path].__lookup != b.children[path].__lookup)
    res = Node.compare(b, a)
    assert(any([path in k for k in res.keys()]))


@pytest.mark.unit
def test__iter_node(SimpleSchema):
    count = sum([1 for i in SimpleSchema.iter_children()])
    assert(count == 15)


@pytest.mark.unit
def test__find_child_attr(SimpleSchema):
    matches = [i for i in SimpleSchema.find_children(
        {'has_attr': ['default']})
    ]
    assert(len(matches) == 2)


@pytest.mark.unit
def test__find_child_attr__failure(SimpleSchema):
    matches = [i for i in SimpleSchema.find_children(
        {'has_attr': ['non-existant']})
    ]
    assert(len(matches) == 0)


@pytest.mark.unit
def test__find_child_match_attr(SimpleSchema):
    matches = [i for i in SimpleSchema.find_children(
        {'match_attr': [{'name': '_id'}]})
    ]
    assert(len(matches) == 1)


@pytest.mark.unit
def test__find_child_match_attr__failure(SimpleSchema):
    matches = [i for i in SimpleSchema.find_children(
        {'match_attr': [{'name': 'not-available'}]})
    ]
    assert(len(matches) == 0)


@pytest.mark.unit
def test__get_child_from_path(SimpleSchema):
    node = SimpleSchema.get_node('rapidtest.Location.latitude')
    assert(node.name == 'latitude')


@pytest.mark.unit
def test__get_child_from_path__fail(SimpleSchema):
    with pytest.raises(ValueError):
        SimpleSchema.get_node('rapidtest.Location.missing')


@pytest.mark.unit
def test__collect_nodes_by_required(ComplexSchema):
    matches = ComplexSchema.collect_matching(
        {'match_attr': [{'optional': False}]}
    )
    count = sum([1 for (path, node) in matches])
    assert(count == 5)


@pytest.mark.unit
def test__collect_nodes_by_optional(ComplexSchema):
    expected = 50
    matches = ComplexSchema.collect_matching(
        {'match_attr': [{'optional': True}]}
    )
    count = sum([1 for (path, node) in matches])
    assert(count == expected)
    # Do it in another way
    matches = ComplexSchema.collect_matching(
        {'attr_contains': [{'avro_type': 'null'}]}
    )
    count = sum([1 for (path, node) in matches])
    assert(count == expected)


@pytest.mark.unit
def test__collect_nodes_by_criteria(ComplexSchema):
    matches = ComplexSchema.collect_matching(
        {'match_attr': [{'__extended_type': 'select1'}]}
    )
    count = sum([1 for (path, node) in matches])
    assert(count == 9)


@pytest.mark.unit
def test__collect_nodes_by_criteria__fail(ComplexSchema):
    matches = ComplexSchema.collect_matching(
        {'match_attr': [{'__extended_type': 'select_seven'}]}
    )
    count = sum([1 for (path, node) in matches])
    assert(count == 0)

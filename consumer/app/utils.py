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

from hashlib import md5
import json


def replace_nested(_dict, keys, value, replace_missing=True):
    if len(keys) > 1:
        try:
            _dict[keys[0]] = replace_nested(
                _dict[keys[0]],
                keys[1:],
                value,
                replace_missing
            )
        except KeyError as ker:
            if replace_missing:
                _dict[keys[0]] = {}
                _dict[keys[0]] = replace_nested(
                    _dict[keys[0]],
                    keys[1:],
                    value,
                    replace_missing
                )
            else:
                raise ker
    else:
        _dict[keys[0]] = value
    return _dict


def hash(obj):
    try:
        _sorted = json.dumps(obj, sort_keys=True)
    except Exception:
        _sorted = str(obj)
    encoded = _sorted.encode('utf-8')
    hash = str(md5(encoded).hexdigest())[:16]  # 64bit hash
    return hash

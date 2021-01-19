#!/usr/bin/env bash
#
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
#

set -Eeuo pipefail

trap '_on_exit' EXIT
trap '_on_err' ERROR

function _on_exit () {
  docker-compose -f docker-compose-test.yml down -v
}

function _on_err () {
  echo "-------------------------"
  docker-compose -f docker-compose-test.yml logs redis
  echo "-------------------------"
  docker-compose -f docker-compose-test.yml logs elasticsearch
  echo "-------------------------"
  docker-compose -f docker-compose-test.yml logs kibana
  echo "-------------------------"

  exit 1
}

docker-compose -f docker-compose-test.yml up -d elasticsearch kibana redis

docker-compose -f docker-compose-test.yml run --rm consumer-test test_integration

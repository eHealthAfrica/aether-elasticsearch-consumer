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

function _on_exit () {
  $DC_TEST down -v
}

function _on_err () {
  echo $LINE
  $DC_TEST logs redis
  echo $LINE
  $DC_TEST logs elasticsearch
  echo $LINE
  $DC_TEST logs kibana
  echo $LINE
  $DC_TEST logs kafka
  echo $LINE
  $DC_TEST logs zookeeper
  echo $LINE

  exit 1
}

trap '_on_exit' EXIT
trap '_on_err' ERR

DC_TEST="docker-compose -f docker-compose-test.yml"
LINE="---------------------------------------------------------------------------"

echo $LINE
$DC_TEST up -d zookeeper kafka
$DC_TEST up -d elasticsearch kibana redis

$DC_TEST build consumer-test

echo $LINE
$DC_TEST run --rm consumer-test test_all
echo $LINE

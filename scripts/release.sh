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

function echo_msg {
  LINE="==============="

  echo -e ""
  echo -e "\e[2m${LINE}\e[0m ${1}: \e[1;92m${TAG}\e[0m \e[2m${LINE}\e[0m"
  echo -e ""
}

# Build and release docker image
IMAGE_REPO="ehealthafrica"
APP="aether-elasticsearch-consumer"
VERSION=$TRAVIS_TAG
TAG="${IMAGE_REPO}/${APP}:${VERSION}"


echo_msg "Building image"
docker build \
    --tag $TAG \
    --build-arg VERSION=$VERSION \
    --build-arg REVISION=$TRAVIS_COMMIT \
    ./consumer
echo_msg "Built image"

echo_msg "Pushing image"
docker push $TAG
echo_msg "Pushed image"

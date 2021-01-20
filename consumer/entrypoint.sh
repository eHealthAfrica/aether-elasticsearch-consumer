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


# Define help message
show_help() {
    echo """
    Commands
    ----------------------------------------------------------------------------
    bash          : run bash
    build         : build python wheel of library in /dist
    eval          : eval shell command

    pip_freeze    : freeze pip dependencies and write to requirements.txt

    start         : run application

    test_unit     : run tests
    test_lint     : run flake8 tests
    test_coverage : run tests with coverage output

    """
}

clean_test() {
    rm -rf .pytest_cache     || true
    rm -rf tests/__pycache__ || true
}

test_flake8() {
    flake8
}

test_unit() {
    clean_test

    pytest -m unit
    cat /code/conf/extras/good_job.txt

    clean_test
}

test_integration() {
    clean_test

    pytest -m integration
    cat /code/conf/extras/good_job.txt

    clean_test
}

case "$1" in
    bash )
        bash
    ;;

    eval )
        eval "${@:2}"
    ;;


    pip_freeze )
        rm -rf /tmp/env
        pip3 install -r ./conf/pip/primary-requirements.txt --upgrade

        cat /code/conf/pip/requirements_header.txt | tee conf/pip/requirements.txt
        pip freeze --local | grep -v appdir | tee -a conf/pip/requirements.txt
    ;;

    start )
        python manage.py "${@:2}"
    ;;

    test_lint )
        test_flake8
    ;;

    test_unit )
        test_flake8
        test_unit "${@:2}"
    ;;

    test_integration )
        test_flake8
        test_integration "${@:2}"
    ;;

    test_all )
        test_flake8
        test_unit "${@:2}"
        test_integration "${@:2}"
    ;;

    build )
        # remove previous build if needed
        rm -rf dist
        rm -rf build
        rm -rf .eggs
        rm -rf aether-sdk-example.egg-info

        # create the distribution
        python setup.py bdist_wheel --universal

        # remove useless content
        rm -rf build
        rm -rf myconsumer.egg-info
    ;;

    help )
        show_help
    ;;

    * )
        show_help
    ;;
esac

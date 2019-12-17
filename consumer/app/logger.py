#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
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

from datetime import datetime
import inspect
import logging

from . import config

consumer_config = config.get_consumer_config()


def get_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        f'%(asctime)s [ES][{name}] %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    level = logging.getLevelName(consumer_config.get('log_level', 'DEBUG'))
    logger.setLevel(level)
    return logger


# CallbackLogger can be used by instances to create a logger for themselves with a memory
# that can be reported

def _make_stack_decorator(stack, max_held, allowed=[]):
    def wrap_logged(fn):
        def _call(*args, **kwargs):
            if fn.__name__ in allowed:
                stack.append([
                    datetime.now().isoformat()[:22],
                    fn.__name__,
                    args[0]]
                )
                _overflow = len(stack) - max_held
                if _overflow > 0:
                    del stack[:_overflow]
            return fn(*args, **kwargs)
        return _call
    return wrap_logged


def _for_all_methods(decorator):
    def decorate(cls):
        for name, fn in inspect.getmembers(cls, inspect.ismethod):
            setattr(cls, name, decorator(fn))
        return cls
    return decorate


def callback_logger(
    name,
    callback_list,
    max_held=100,
    logged_events=[
        'error',
        'debug',
        'warn',
        'info'
    ]
):
    _logger = get_logger(name)
    stack_decorator = _make_stack_decorator(callback_list, max_held, logged_events)
    wrapper = _for_all_methods(stack_decorator)
    return wrapper(_logger)

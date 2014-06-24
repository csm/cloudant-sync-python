# DocumentBody.py
# Copyright (C) 2014 Memeo Inc.
#
# Based off of Android version, in turn off the iOS version.
#   * Original iOS version by Jens Alfke, ported to Android by Marty Schoch
#   * Original also (C) 2012 Couchbase, Inc.
#   * Adapted and (C) 2013 by Cloudant, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

import json


class DocumentBody(object):
    def __init__(self, dict_value=None, bytes_value=None):
        if dict_value is None and bytes_value is None:
            raise ValueError('require at least one of dict_value or bytes_value')
        if bytes_value is not None:
            if not isinstance(bytes_value, bytes):
                raise ValueError('bytes_value must be of type bytes')
            if not isinstance(json.loads(bytes_value), dict):
                raise ValueError('bytes_value must be a JSON object')
            self.__bytes = bytes_value
        else:
            self.__bytes = None
        if dict_value is not None:
            # TODO test if dict_value is dict-like
            self.__dict = dict_value
        else:
            self.__dict = None

    def to_bytes(self):
        if self.__bytes is None:
            self.__bytes = json.dumps(self.__dict)
        return self.__bytes

    def to_dict(self):
        if self.__dict is None:
            self.__dict = json.loads(self.__bytes)
        return self.__dict

    def __str__(self):
        if self.__bytes is not None:
            return self.__bytes
        return json.dumps(self.__dict)

    def __repr__(self):
        if self.__dict is not None:
            return 'DocumentBody(dict_value=%r)' % self.__dict
        return 'DocumentBody(bytes_value=%r)' % self.__bytes
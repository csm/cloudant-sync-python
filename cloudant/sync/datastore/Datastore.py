# Datastore.py
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

import os
import sqlite3

from .Database import Database
from .DatabaseConstants import *
from .DocumentRevision import DocumentRevision

class Datastore(object):
    DB_FILE_NAME = 'db.sync'

    def __init__(self, path, name, on_stmt=None):
        if path is None or not isinstance(path, basestring):
            raise ValueError("path must be a string")
        if name is None or not isinstance(name, basestring):
            raise ValueError("name must be a string")
        self.__path = path
        self.__name = name
        self.__extensions_dir = os.path.join(path, "extensions")
        dbfile = os.path.join(path, self.DB_FILE_NAME)
        self.__db = Database(sqlite3.connect(dbfile), on_stmt)
        self.__update_schema(SCHEMA_VERSION_3, 3)
        self.__update_schema(getSCHEMA_VERSION_4(), 4)

    def __update_schema(self, schema, version):
        self.__db.execute('PRAGMA foreign_keys = ON;')
        dbversion = self.__db.get_version()
        if dbversion < version:
            try:
                self.__db.begin_transaction()
                for statement in schema:
                    self.__db.execute(statement)
                self.__db.execute('PRAGMA user_version = %s;' % version)
                self.__db.set_transaction_success()
            finally:
                self.__db.end_transaction()

    def get_last_sequence(self):
        return self.__db.execute('SELECT max(sequence) FROM revs;').fetchone()[0]

    def __len__(self):
        return self.__db.execute('SELECT count(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0').fetchone()[0]

    def __contains__(self, item):
        return self.get(item) is not None

    def get(self, id, rev=None):
        if rev is None:
            args = (id,)
            sql = 'SELECT docs.docid, docs.doc_id, revid, sequence, json, current, deleted, parent FROM revs, docs' \
                  ' WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND current=1 ORDER BY revid DESC LIMIT 1'
        else:
            args = (id, rev)
            sql = 'SELECT docs.docid, docs.doc_id, revid, sequence, json, current, deleted, parent FROM revs, docs' \
                  ' WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1'
        c = self.__db.execute(sql, args)
        return DocumentRevision.from_cursor(c)



    name = property(lambda self: self.__name)
    last_sequence = property(get_last_sequence)


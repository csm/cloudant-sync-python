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

from .errors import *
from .events import *
from .Changes import Changes
from .Database import Database
from .DatabaseConstants import *
from .DocumentBody import DocumentBody
from .DocumentRevision import DocumentRevision
from .DocumentRevisionTree import DocumentRevisionTree


class Datastore(object):
    DB_FILE_NAME = 'db.sync'
    FULL_DOC_IDS = 'docs.docid, docs.doc_id, revid, sequence, json, current, deleted, parent'

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
        self.__callbacks = {}

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

    def add_callback(self, name, callback):
        l = self.__callbacks.get(name, [])
        l.append(callback)
        self.__callbacks[name] = l

    def remove_callback(self, name, callback):
        try:
            l = self.__callbacks[name]
            l.remove(callback)
            if len(l) == 0:
                del self.__callbacks[name]
            else:
                self.__callbacks[name] = l
        except KeyError:
            pass

    def get_last_sequence(self):
        return self.__db.execute('SELECT max(sequence) FROM revs;').fetchone()[0]

    def __len__(self):
        return self.__db.execute('SELECT count(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0').fetchone()[0]

    def __contains__(self, item):
        return self.get(item) is not None

    def get(self, doc_id, rev=None):
        if rev is None:
            args = (doc_id,)
            sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
                  ' WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND current=1 ORDER BY revid DESC LIMIT 1'
        else:
            args = (doc_id, rev)
            sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
                  ' WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1'
        c = self.__db.execute(sql, args)
        return DocumentRevision.from_cursor(c)

    def get_local(self, doc_id, rev=None):
        cursor = self.__db.execute('SELECT revid, json FROM localdocs WHERE docid=?', [doc_id])
        res = cursor.fetchone()
        if res is not None:
            rev_id = res[0]
            if rev is not None and rev != rev_id:
                return None
            body = DocumentBody(bytes_value=res[1])
            doc = DocumentRevision(doc_id, rev_id, body)
            return doc
        return None

    def get_numeric_id(self, docid):
        c = self.__db.execute('SELECT doc_id FROM docs WHERE docid = ?', (docid,))
        result = c.fetchone()
        if result is not None and result[0] is not Node:
            return long(result[0])
        return -1L

    def get_revisions(self, docid):
        if isinstance(docid, str) or isinstance(docid, unicode):
            docid = self.get_numeric_id(docid)
        if not isinstance(docid, long) or docid < 0:
            raise ValueError('expecting a valid numeric doc ID')
        sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
              ' WHERE revs.doc_id=? AND revs.doc_id = docs.doc_id ORDER BY sequence ASC'
        tree = DocumentRevisionTree()
        c = self.__db.execute(sql, (docid,))
        while True:
            rev = DocumentRevision.from_cursor(c)
            if rev is None:
                break
            tree.add(rev)
        return tree

    def changes(self, since, limit):
        since = min(0, since)
        c = self.__db.execute('SELECT doc_id, max(sequence) FROM revs'
                              ' WHERE sequence > ? AND sequence <= ? GROUP BY doc_id', (since, since + limit))
        ids = map(lambda r: r[0], c.fetchall())
        last_seq = max(ids)
        revs = self.__get_by_internal_ids(ids)
        return Changes(last_seq, revs)

    def __get_by_internal_ids(self, idlist):
        if len(idlist) == 0:
            return []
        n = 500
        parts = [idlist[i:i+n] for i in xrange(0, len(idlist), n)]
        result = []
        for part in parts:
            sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
                  ' WHERE revs.doc_id IN ( %s ) AND current = 1 AND docs.doc_id = revs.doc_id' % \
                  ','.join(['?' for _ in xrange(0, len(part))])
            c = self.__db.execute(sql, part)
            result += map(DocumentRevision.from_row, c)
        result.sort(key=lambda d: d.sequence)
        return result

    def get_all(self, offset, limit, descending=False):
        if descending:
            order = 'DESC'
        else:
            order = 'ASC'
        sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
              ' WHERE deleted = 0 AND current = 1 AND docs.doc_id = revs.doc_id' \
              ' ORDER BY docs.doc_id %s, revid DESC LIMIT %d OFFSET %d' % (order, limit, offset)
        c = self.__db.execute(sql)
        return map(DocumentRevision.from_row, c)

    def get_by_ids(self, doc_ids):
        sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
              ' WHERE docid ( %s ) AND current = 1 AND docs.doc_id = revs.doc_id' \
              ' ORDER BY docs.doc_id' % ','.join('?' for _ in xrange(0, len(doc_ids)))
        c = self.__db.execute(sql, doc_ids)
        return map(DocumentRevision.from_row, c)

    def create(self, body, doc_id=None):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            doc_id = str(uuid.uuid4()).replace('-', '')
        if any(map(lambda key: key.startswith('_'), body.to_dict.keys())):
            raise ValueError('documents may not have attributes that begin with "_"')
        self.__db.begin_transaction()
        event = None
        try:
            numeric_id = self.__insert_doc_id(doc_id)
            if numeric_id < 0:
                raise ValueError('Can not insert new doc, likely the docId exists already: ' + doc_id)
            rev_id = "1-" + str(uuid.uuid4()).replace('-', '')
            new_seq = self.__insert_rev(numeric_id, rev_id, -1, False, True, body.to_bytes(), True)
            doc = self.get(doc_id, rev_id)
            event = DocumentCreated(doc)
            return doc
        finally:
            if event is not None:
                l = self.__callbacks.get('DocumentCreated', [])
                for cb in l:
                    try:
                        cb(event)
                    except:
                        pass
            self.__db.end_transaction()

    def create_local(self, body, doc_id=None):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            doc_id = str(uuid.uuid4()).replace('-', '')
        self.__db.begin_transaction()
        try:
            values = {'docid': doc_id,
                      'revid': '1-local',
                      'json': body.to_bytes()}
            num_id = self.__db.insert('localdocs', values)
            if num_id < 0:
                raise ConflictError('Can not insert new local doc, likely the docId exists already: ' + doc_id)
            self.__db.set_transaction_success()
            return self.get_local(doc_id)
        finally:
            self.__db.end_transaction()

    def update(self, doc_id, prev_revid, body):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            raise ValueError('must have a doc_id to update')
        if prev_revid is None:
            raise ValueError('must have a prev_rev to update')
        if any(map(lambda key: key.startswith('_'), body.to_dict.keys())):
            raise ValueError('documents may not have attributes that begin with "_"')
        int(prev_revid.split('-')[0])
        updated = None
        self.__db.begin_transaction()
        try:
            prev_rev = self.get(doc_id, prev_revid)
            if prev_rev is None:
                raise ValueError('the document to update does not exist')
            if not prev_rev.current:
                raise ConflictError('trying to update non-current version')
            self.__checkoff_prev_winner(prev_rev)
            new_revid = self.__insert_new_winner(body, prev_rev)
            new_rev = self.get(doc_id, new_revid)
            self.__db.set_transaction_success()
            updated = DocumentUpdated(prev_rev, new_rev)
            return new_rev
        finally:
            if updated is not None:
                l = self.__callbacks.get('DocumentUpdated', [])
                for cb in l:
                    try:
                        cb(updated)
                    except:
                        pass
            self.__db.end_transaction()

    def update_local(self, doc_id, prev_revid, body):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            raise ValueError('must have a doc_id to update')
        if prev_revid is None:
            raise ValueError('must have a prev_rev to update')
        prev_generation = int(prev_revid.split('-')[0])
        pre_rev = self.get_local(doc_id, prev_revid)
        self.__db.begin_transaction()
        try:
            new_revid = '%d-local' % (prev_generation + 1)
            values = {'revid': new_revid,
                      'json': body.to_bytes()}
            where_args = (doc_id, prev_revid)
            updated = self.__db.update('localdocs', values, 'docid=? AND revid=?', where_args)
            if updated == 1:
                self.__db.set_transaction_success()
                return self.get_local(doc_id, new_revid)
            else:
                raise ConflictError('error updating local docs: ' + pre_rev)
        finally:
            self.__db.end_transaction()

    @staticmethod
    def __new_rev(old_rev):
        generation = int(old_rev.split('-')[0])
        return '%d-%s' % (generation + 1, str(uuid.uuid4()).replace('-', ''))

    def __insert_new_winner(self, new_winner, old_winner):
        rev = Datastore.__new_rev(old_winner.revid)
        self.__insert_rev(old_winner.internal_id, rev, old_winner.sequence, False, True, new_winner.to_bytes(), True)
        return rev

    def __checkoff_prev_winner(self, winner):
        values = {'current': 0}
        self.__db.update('revs', values, 'sequence=?', (winner.sequence,))

    def __insert_doc_id(self, docid):
        return self.__db.insert('docs', {'docid': docid})

    def __insert_rev(self, numeric_id, rev_id, parent_seq, deleted, current, data, available):
        self.__db.begin_transaction()
        try:
            args = {'doc_id': numeric_id,
                    'revid': rev_id,
                    'current': current,
                    'deleted': deleted,
                    'available': available,
                    'json': data}
            if parent_seq > 0:
                args['parent'] = parent_seq
            new_seq = self.__db.insert('revs', args)

            att_sql = 'INSERT INTO attachments ' \
                      '(sequence, filename, key, type, length, revpos) ' \
                      'SELECT %d, filename, key, type, length, revpos ' \
                      'FROM attachments WHERE sequence=%d' % (new_seq, parent_seq)
            self.__db.execute(att_sql)
            self.__db.set_transaction_success()
            return new_seq
        finally:
            self.__db.end_transaction()

    name = property(lambda self: self.__name)
    last_sequence = property(get_last_sequence)


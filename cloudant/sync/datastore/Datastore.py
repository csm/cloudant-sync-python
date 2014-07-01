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

import base64
import collections
import itertools
import logging
import os
import sqlite3
import tempfile
import threading

from .Attachment import UnsavedBytesAttachment
from .AttachmentManager import AttachmentManager, PreparedAttachment
from .errors import *
from .events import *
from .Changes import Changes
from .Database import Database
from .DatabaseConstants import *
from .DocumentBody import DocumentBody
from .DocumentRevision import DocumentRevision
from .DocumentRevisionTree import DocumentRevisionTree


class Datastore(object):
    """
    A local data store.
    """

    DB_FILE_NAME = 'db.sync'
    FULL_DOC_IDS = 'docs.docid, docs.doc_id, revid, sequence, json, current, deleted, parent'

    def __init__(self, path, name, on_stmt=None, in_memory=False):
        """
        Creates a new datastore.

        If not creating an in-memory database, the path argument must be unique for the database
        you are creating. That is, if you were to create two datastores test1 and test2, you should
        do something like this:

        >>> d1 = Datastore('/path/to/databases/test1', 'test1')
        >>> d2 = Datastore('/path/to/databases/test2', 'test2')

        Because, this constructor will place all data for this database directly beneath
        the path you specify.

        Args:
            path: the directory to store the database file; must exist.
            name: the name of this database.
            on_stmt: an optional callback, which will be passed each SQLite statement
                and argument list executed.
            in_memory: if True, then use a :memory: database, and ignore path. Note
                that in-memory datastores may only be used within a single thread.
        """
        if not in_memory:
            if path is None or not isinstance(path, basestring):
                raise ValueError("path must be a string")
            if name is None or not isinstance(name, basestring):
                raise ValueError("name must be a string")
            self.__path = path
            self.__name = name
            self.__extensions_dir = os.path.join(path, "extensions")
            self.__dbfile = os.path.join(path, self.DB_FILE_NAME)
        else:
            self.__path = ':memory:'
            self.__name = name
            self.__extensions_dir = None
            self.__dbfile = ':memory:'
        self.__onstmt = on_stmt
        self.__dblocal = threading.local()
        self.__update_schema(SCHEMA_VERSION_3, 3)
        self.__update_schema(getSCHEMA_VERSION_4(), 4)
        self.__callbacks = {}
        self.__attachment_manager = AttachmentManager(self)

    def __get_connection(self):
        try:
            return self.__dblocal.db
        except AttributeError:
            self.__dblocal.db = Database(sqlite3.connect(self.__dbfile, check_same_thread=False), self.__onstmt)
            return self.__dblocal.db

    def __update_schema(self, schema, version):
        db = self.__get_connection()
        db.execute('PRAGMA foreign_keys = ON;')
        dbversion = db.get_version()
        logging.getLogger('cloudant.sync.datastore').info('checking schema version %r database version %r',
                                                          version, dbversion)
        if dbversion < version:
            try:
                db.begin_transaction()
                for statement in schema:
                    db.execute(statement)
                db.execute('PRAGMA user_version = %s;' % version)
                db.set_transaction_success()
            finally:
                db.end_transaction()

    def extension_data_dir(self, extension):
        if self.__extensions_dir is not None:
            return os.path.join(self.__extensions_dir, extension)
        return os.path.join(tempfile.gettempdir(), extension)

    def add_callback(self, name, callback):
        """
        Add a callback, which is invoked when various things occur.

        Args:
            name: The callback name; may be one of the following:
                DocumentCreated: called when a new document is created.
                DocumentDeleted: called when a document is deleted.
                DocumentModified: called when an existing document is modified.
            callback: a callable that should accept a single event argument,
                which will be a DocumentCreated, DocumentDeleted, or a
                DocumentModified instance.
        """
        l = self.__callbacks.get(name, [])
        l.append(callback)
        self.__callbacks[name] = l

    def remove_callback(self, name, callback):
        """
        Remove a callback.
        """
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
        return self.__get_connection().execute('SELECT max(sequence) FROM revs;').fetchone()[0]

    def __len__(self):
        return self.__get_connection().execute('SELECT count(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0').fetchone()[0]

    def __contains__(self, item):
        return self.get(item) is not None

    def get_sql(self):
        return self.__get_connection()

    def get(self, doc_id, rev=None):
        """
        Fetch a document.

        Args:
            doc_id: the ID of the document.
            rev: the revision of the document to fetch. If not given, the current revision is returned.
        Returns:
            A DocumentRevision object containing the document, or None if the document was not found.
        """
        if rev is None:
            args = (doc_id,)
            sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
                  ' WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND current=1 ORDER BY revid DESC LIMIT 1'
        else:
            args = (doc_id, rev)
            sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
                  ' WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1'
        c = self.__get_connection().execute(sql, args)
        return DocumentRevision.from_cursor(c)

    def get_local(self, doc_id, rev=None):
        """
        Fetch a local document.

        """
        cursor = self.__get_connection().execute('SELECT revid, json FROM localdocs WHERE docid=?', [doc_id])
        res = cursor.fetchone()
        if res is not None:
            rev_id = res[0]
            if rev is not None and rev != rev_id:
                return None
            body = DocumentBody(bytes_value=bytes(res[1]))
            doc = DocumentRevision(doc_id, rev_id, body)
            return doc
        return None

    def get_numeric_id(self, docid):
        c = self.__get_connection().execute('SELECT doc_id FROM docs WHERE docid = ?', (docid,))
        result = c.fetchone()
        if result is not None and result[0] is not None:
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
        c = self.__get_connection().execute(sql, (docid,))
        while True:
            rev = DocumentRevision.from_cursor(c)
            if rev is None:
                break
            tree.add(rev)
        return tree

    def changes(self, since, limit):
        """
        Fetch changes that have occurred to this datastore.

        Args:
            since: the sequence number whence to start fetching changes.
            limit: the maximum number of changes to fetch.
        """
        since = min(0, since)
        c = self.__get_connection().execute('SELECT doc_id, max(sequence) FROM revs'
                                            ' WHERE sequence > ? AND sequence <= ? GROUP BY doc_id',
                                            (since, since + limit))
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
            c = self.__get_connection().execute(sql, part)
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
        c = self.__get_connection().execute(sql)
        return map(DocumentRevision.from_row, c)

    def get_by_ids(self, doc_ids):
        sql = 'SELECT ' + self.FULL_DOC_IDS + ' FROM revs, docs' \
              ' WHERE docid ( %s ) AND current = 1 AND docs.doc_id = revs.doc_id' \
              ' ORDER BY docs.doc_id' % ','.join('?' for _ in xrange(0, len(doc_ids)))
        c = self.__get_connection().execute(sql, doc_ids)
        return map(DocumentRevision.from_row, c)

    def create(self, body, doc_id=None):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            doc_id = str(uuid.uuid4()).replace('-', '')
        if any(map(lambda key: key.startswith('_'), body.to_dict().keys())):
            raise ValueError('documents may not have attributes that begin with "_"')
        db = self.__get_connection()
        db.begin_transaction()
        event = None
        try:
            numeric_id = self.__insert_doc_id(doc_id)
            if numeric_id < 0:
                raise ValueError('Can not insert new doc, likely the docId exists already: ' + doc_id)
            rev_id = "1-" + str(uuid.uuid4()).replace('-', '')
            new_seq = self.__insert_rev(numeric_id, rev_id, -1, False, True, body.to_bytes(), True)
            doc = self.get(doc_id, rev_id)
            event = DocumentCreated(doc)
            db.set_transaction_success()
            return doc
        except sqlite3.IntegrityError:
            raise ConflictError('document with that ID already exists')
        finally:
            self.__invoke_callbacks('DocumentCreated', event)
            db.end_transaction()

    def create_local(self, body, doc_id=None):
        """
        Create a local document. Local documents behave similarly to normal documents, but are not replicated.

        Args:
            body: A DocumentBody object giving the body of the new document.
            doc_id: The document identifier. If omitted, a random identifier will be assigned.
        Returns:
            The DocumentRevision for the document that was just created.
        """
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            doc_id = str(uuid.uuid4()).replace('-', '')
        db = self.__get_connection()
        db.begin_transaction()
        try:
            values = {'docid': doc_id,
                      'revid': '1-local',
                      'json': body.to_bytes()}
            num_id = db.insert('localdocs', values)
            if num_id < 0:
                raise ConflictError('Can not insert new local doc, likely the docId exists already: ' + doc_id)
            db.set_transaction_success()
            return self.get_local(doc_id)
        finally:
            db.end_transaction()

    def update(self, doc_id, prev_revid, body):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            raise ValueError('must have a doc_id to update')
        if prev_revid is None:
            raise ValueError('must have a prev_rev to update')
        if any(map(lambda key: key.startswith('_'), body.to_dict().keys())):
            raise ValueError('documents may not have attributes that begin with "_"')
        int(prev_revid.split('-')[0])
        updated = None
        db = self.__get_connection()
        db.begin_transaction()
        try:
            prev_rev = self.get(doc_id, prev_revid)
            if prev_rev is None:
                raise ValueError('the document to update does not exist')
            if not prev_rev.current:
                raise ConflictError('trying to update non-current version')
            self.__checkoff_prev_winner(prev_rev)
            new_revid = self.__insert_new_winner(body, prev_rev)
            new_rev = self.get(doc_id, new_revid)
            db.set_transaction_success()
            updated = DocumentUpdated(prev_rev, new_rev)
            return new_rev
        finally:
            self.__invoke_callbacks('DocumentUpdated', updated)
            db.end_transaction()

    def update_local(self, doc_id, prev_revid, body):
        if not isinstance(body, DocumentBody):
            raise ValueError('expected a DocumentBody')
        if doc_id is None:
            raise ValueError('must have a doc_id to update')
        if prev_revid is None:
            raise ValueError('must have a prev_rev to update')
        prev_generation = int(prev_revid.split('-')[0])
        pre_rev = self.get_local(doc_id, prev_revid)
        db = self.__get_connection()
        db.begin_transaction()
        try:
            new_revid = '%d-local' % (prev_generation + 1)
            values = {'revid': new_revid,
                      'json': body.to_bytes()}
            where_args = (doc_id, prev_revid)
            updated = db.update('localdocs', values, 'docid=? AND revid=?', where_args)
            if updated == 1:
                db.set_transaction_success()
                return self.get_local(doc_id, new_revid)
            else:
                raise ConflictError('error updating local docs: ' + pre_rev)
        finally:
            db.end_transaction()

    def delete(self, doc_id, prev_revid):
        db = self.__get_connection()
        if db.is_closed():
            raise ValueError('database is closed')
        if doc_id is None or len(doc_id) == 0:
            raise ValueError('must have a doc_id to delete')
        if prev_revid is None or len(prev_revid) == 0:
            raise ValueError('must have a prev_revid to delete')
        doc_deleted = None
        db.begin_transaction()
        try:
            pre_rev = self.get(doc_id, prev_revid)
            if pre_rev is None:
                raise ConflictError('document to delete does not exist')
            rev_tree = self.get_revisions(doc_id)
            if rev_tree is None:
                raise ConflictError('document does not exist for id: ' + doc_id)
            elif prev_revid not in rev_tree.leaf_revids():
                raise ConflictError('revision to be deleted is not a leaf revision: ' + prev_revid)
            if not pre_rev.deleted:
                self.__checkoff_prev_winner(pre_rev)
                new_revid = Datastore.__new_rev(prev_revid)
                self.__insert_rev(pre_rev.internal_id, new_revid, pre_rev.sequence, True, pre_rev.current, '{}', False)
                new_rev = self.get(doc_id, new_revid)
                doc_deleted = DocumentDeleted(pre_rev, new_rev)
            db.set_transaction_success()
        finally:
            self.__invoke_callbacks('DocumentDeleted', doc_deleted)
            db.end_transaction()

    def delete_local(self, doc_id):
        if doc_id is None or len(doc_id) == 0:
            raise ValueError('document ID cannot be empty')
        rows_del = self.__get_connection().delete('localdocs', 'docid=?', (doc_id,))
        if rows_del == 0:
            raise DocumentNotFoundError('no local doc with id: ' + doc_id)

    def get_public_id(self):
        c = self.__get_connection().execute('SELECT value FROM info WHERE key=\'publicUUID\'')
        row = c.fetchone()
        if row is None:
            raise ValueError('failed to query publicUUID; is the DB initialized?')
        return 'touchdb_' + str(row[0])

    def force_insert(self, doc_rev, rev_history, attachments):
        if not isinstance(doc_rev, DocumentRevision):
            raise ValueError('doc_rev must be a DocumentRevision')
        if rev_history is None or len(rev_history) == 0 or rev_history[-1] != doc_rev.revid:
            raise ValueError('rev_history must be non empty, and must end with the document\'s rev')
        a, b = itertools.tee(rev_history)
        next(b, None)
        if any(map(lambda pair: Datastore.__gen_from_rev(pair[0]) >= Datastore.__gen_from_rev(pair[1]),
                   itertools.izip(a, b))):
            raise ValueError('rev_history must be in order')
        doc_created = None
        doc_updated = None
        ok = True
        db = self.__get_connection()
        db.begin_transaction()
        try:
            if doc_rev.docid in self:
                seq = self.__do_force_insert(doc_rev, rev_history)
                doc_rev._DocumentRevision_sequence = seq
                doc_updated = DocumentUpdated(None, doc_rev)
            else:
                seq = self.__do_force_create(doc_rev, rev_history)
                doc_rev._DocumentRevision_sequence = seq
                doc_created = DocumentCreated(doc_rev)
            if attachments is not None:
                for key, value in attachments.items():
                    data = base64.b64decode(value['data'])
                    type = value['content_type']
                    att = UnsavedBytesAttachment(key, type, data)
                    result = False
                    try:
                        prepped = PreparedAttachment(att)
                        result = self.__attachment_manager.add_attachment(prepped, doc_rev)
                    except:
                        pass
                    if not result:
                        ok = False
                        break
            if ok:
                db.set_transaction_success()
        finally:
            self.__invoke_callbacks('DocumentCreated', doc_created)
            self.__invoke_callbacks('DocumentUpdated', doc_updated)
            db.end_transaction()

    def revs_diff(self, revs):
        missing_revs = collections.defaultdict(list)
        batches = self.__multimap_partitions(revs, 500)
        for batch in batches:
            batch = self.__revs_diff_batch(batch)
            for key, values in batch.iteritems():
                missing_revs[key] = values
        return missing_revs

    def __multimap_partitions(self, multimap, limit=500):
        maps = []
        current = collections.defaultdict(list)
        for key, values in multimap.iteritems():
            current[key] = values
            if sum(map(len, current.values())) + len(current) >= limit:
                maps.append(current)
                current = collections.defaultdict(list)
        if len(current) > 0:
            maps.append(current)
        return maps

    def __revs_diff_batch(self, batch):
        sql = "SELECT docs.docid, revs.revid FROM docs, revs " \
              "WHERE docs.doc_id = revs.doc_id AND docs.docid IN (%s) AND revs.revid IN (%s) " \
              "ORDER BY docs.docid" % (','.join(itertools.repeat('?', len(batch))), ','.join(itertools.repeat('?', sum(map(len, batch.values())))))
        db = self.__get_connection()
        l = list(batch.keys()) + reduce(lambda x, y: x + y, batch.values())
        logging.getLogger('cloudant.sync.datastore').debug('rev_diff batch args: %r', l)
        c = db.execute(sql, l)
        for row in c:
            batch.get(row[0], []).remove(row[1])
        return batch

    def __do_force_create(self, doc_rev, rev_history):
        assert not doc_rev.docid in self
        numeric_id = self.__insert_doc_id(doc_rev.docid)
        parent_seq = 0L
        for rev in rev_history[:-1]:
            parent_seq = self.__insert_stub_rev(numeric_id, rev, parent_seq)
        return self.__insert_rev(numeric_id, rev_history[-1], parent_seq, doc_rev.deleted, True,
                                 doc_rev.body.to_bytes(), True)

    def __do_force_insert(self, doc_rev, rev_history):
        numeric_id = self.get_numeric_id(doc_rev.docid)
        local_revs = self.get_revisions(doc_rev.docid)
        assert local_revs is not None
        parent = local_revs.lookup(doc_rev.docid, rev_history[0])
        if parent is None:
            seq = self.__insert_into_new_tree(doc_rev, rev_history, numeric_id, local_revs)
        else:
            seq = self.__insert_into_existing_tree(doc_rev, rev_history, numeric_id, local_revs)
        return seq

    def __insert_into_new_tree(self, doc_rev, rev_history, numeric_id, local_revs):
        prev_winner = local_revs.current_rev()
        parent_seq = 0L
        for rev in rev_history[:-1]:
            parent_seq = self.__insert_stub_rev(numeric_id, rev, parent_seq)
            new_node = self.get(doc_rev.docid, rev)
            local_revs.add(new_node)
        seq = self.__insert_rev(numeric_id, doc_rev.revid, parent_seq, doc_rev.deleted, True, doc_rev.to_bytes(),
                                not doc_rev.deleted)
        new_leaf = self.get(doc_rev.docid, doc_rev.revid)
        local_revs.add(new_leaf)
        self.__pick_winner_of_conflicts(local_revs, new_leaf, prev_winner)
        return seq

    def __insert_into_existing_tree(self, doc_rev, rev_history, numeric_id, local_revs):
        parent = local_revs.lookup(doc_rev.docid, rev_history[0])
        if parent is None:
            raise ValueError('parent must exist')
        prev_leaf = local_revs.current_rev()
        i = 1
        while i < len(rev_history):
            next_node = local_revs.child_by_rev(parent, rev_history[i])
            if next_node is None:
                break
            else:
                parent = next_node
            i += 1
        if i >= len(rev_history):
            return -1
        while i < len(rev_history) - 1:
            self.__change_to_not_current(parent.sequence)
            self.__insert_stub_rev(numeric_id, rev_history[i], parent.sequence)
            parent = self.get(doc_rev.docid, rev_history[i])
            local_revs.add(parent)
        new_revid = rev_history[-1]
        self.__change_to_not_current(parent.sequence)
        seq = self.__insert_rev(numeric_id, new_revid, parent.sequence, doc_rev.deleted, True, doc_rev.to_bytes(), True)
        new_leaf = self.get(doc_rev.docid, new_revid)
        prev_leaf = self.get(prev_leaf.docid, prev_leaf.revid)
        if prev_leaf.current:
            self.__pick_winner_of_conflicts(local_revs, new_leaf, prev_leaf)
        return seq

    def __insert_stub_rev(self, numeric_id, rev, parent_seq):
        return self.__insert_rev(numeric_id, rev, parent_seq, False, False, '{}', False)

    def __pick_winner_of_conflicts(self, object_tree, new_leaf, previous_leaf):
        if new_leaf.deleted == previous_leaf.deleted:
            prev_leaf_depth = object_tree.depth(previous_leaf.sequence)
            new_leaf_depth = object_tree.depth(new_leaf.sequence)
            if prev_leaf_depth > new_leaf_depth:
                self.__change_to_not_current(new_leaf.sequence)
            elif prev_leaf_depth < new_leaf_depth:
                self.__change_to_not_current(previous_leaf.sequence)
            else:
                prev_rev_hash = previous_leaf.revid
                new_rev_hash = new_leaf.revid
                if prev_rev_hash.split('-')[1] > new_rev_hash.split('-')[1]:
                    self.__change_to_not_current(new_leaf.sequence)
                else:
                    self.__change_to_not_current(previous_leaf.sequence)
        else:
            if new_leaf.deleted:
                self.__change_to_not_current(new_leaf.sequence)
            else:
                self.__change_to_not_current(previous_leaf.sequence)

    def __change_to_not_current(self, seq):
        self.__get_connection().update('revs', {'current': 0}, 'sequence=?', (seq,))

    def __invoke_callbacks(self, tag, value):
        if value is not None:
            l = self.__callbacks.get(tag, [])
            for cb in l:
                try:
                    cb(value)
                except:
                    pass

    @staticmethod
    def __gen_from_rev(rev):
        x = rev.split('-')
        return int(x[0])

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
        self.__get_connection().update('revs', values, 'sequence=?', (winner.sequence,))

    def __insert_doc_id(self, docid):
        return self.__get_connection().insert('docs', {'docid': docid})

    def __insert_rev(self, numeric_id, rev_id, parent_seq, deleted, current, data, available):
        db = self.__get_connection()
        db.begin_transaction()
        try:
            args = {'doc_id': numeric_id,
                    'revid': rev_id,
                    'current': current,
                    'deleted': deleted,
                    'available': available,
                    'json': data}
            if parent_seq > 0:
                args['parent'] = parent_seq
            new_seq = db.insert('revs', args)

            att_sql = 'INSERT INTO attachments ' \
                      '(sequence, filename, key, type, length, revpos) ' \
                      'SELECT %d, filename, key, type, length, revpos ' \
                      'FROM attachments WHERE sequence=%d' % (new_seq, parent_seq)
            db.execute(att_sql)
            db.set_transaction_success()
            return new_seq
        finally:
            db.end_transaction()

    name = property(lambda self: self.__name)
    last_sequence = property(get_last_sequence)


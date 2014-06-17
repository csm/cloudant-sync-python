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

from DocumentBody import DocumentBody
# from cloudant.sync.util.CouchUtils import CouchUtils


class DocumentRevision(object):
    def __init__(self, docid, revid, body=None, sequence=-1L, internal_id=0L, deleted=False, current=True, parent=-1L):
        if not isinstance(docid, str):
            raise ValueError('docid must be of type str')
        if revid is not None and not isinstance(revid, str):
            raise ValueError('revid must be of type str')
        if body is not None and not isinstance(body, DocumentBody):
            raise ValueError('body must be of type DocumentBody, or None')
        if not isinstance(sequence, long):
            raise ValueError('sequence must be of type long')
        if not isinstance(internal_id, long):
            raise ValueError('internal_id must be of type long')
        if not isinstance(deleted, bool):
            raise ValueError('deleted must be of type bool')
        if not isinstance(current, bool):
            raise ValueError('current must be of type bool')
        if not isinstance(parent, long):
            raise ValueError('parent must be of type long')
        self.__docid = docid
        self.__revid = revid
        self.__body = body
        self.__sequence = sequence
        self.__internal_id = internal_id
        self.__deleted = deleted
        self.__current = current
        self.__parent = parent

    @staticmethod
    def from_cursor(c):
        row = c.fetchone()
        if row is None:
            return None
        doc_id, internal_id, rev_id, sequence, json, current, deleted = row
        return DocumentRevision(doc_id, rev_id, DocumentBody(bytes_value=json), sequence, internal_id, deleted > 0, current > 0)

    @property
    def generation(self):
        return None

    @property
    def docid(self):
        return self.__docid

    @property
    def revid(self):
        return self.__revid

    @property
    def body(self):
        return self.__body

    @property
    def sequence(self):
        return self.__sequence

    @property
    def internal_id(self):
        return self.__internal_id

    @property
    def deleted(self):
        return self.__deleted

    @property
    def current(self):
        return self.__current

    @property
    def parent(self):
        return self.__parent

    def to_bytes(self):
        if self.body is not None:
            return self.body.to_bytes()
        return None

    def to_dict(self):
        if self.body is not None:
            d = self.body.to_dict()
            d['_id'] = self.docid
            if self.revid is not None:
                d['_rev'] = self.revid
            if self.deleted:
                d['_deleted'] = True
            return d
        return None

    def __str__(self):
        return '{ id: %s, rev: %s, seq: %d, parent: %d }' % (self.docid, self.revid, self.sequence, self.parent)

    def __hash__(self):
        x = 0
        if self.docid is not None:
            x = hash(self.docid)
        return 31 + x

    def __eq__(self, other):
        if not isinstance(other, DocumentRevision):
            return False
        return self.docid == other.docid

    def __cmp__(self, other):
        if not isinstance(other, DocumentRevision):
            raise ValueError('object to compare must be a DocumentRevision')
        return cmp(self.sequence, other.sequence)
import base64
import tempfile
import hashlib
import os

from .Attachment import Attachment, SavedAttachment
from .DocumentRevision import DocumentRevision

class PreparedAttachment(object):
    def __init__(self, attachment, dir):
        self.__attachment = attachment
        self.__tempfile, self.__tempfilename = tempfile.mkstemp(dir=dir)
        with attachment.get_data() as fin:
            sha = hashlib.sha1()
            data = fin.read()
            sha.update(data)
            self.__tempfile.write(data)
            self.__sha1 = sha.digest()

    def delete_tempfile(self):
        os.unlink(self.__tempfilename)

    @property
    def attachment(self):
        return self.__attachment

    @property
    def tempfile(self):
        return self.__tempfilename

    @property
    def sha1(self):
        return self.__sha1


class AttachmentManager(object):
    PLAIN_ENCODING = 0
    GZIP_ENCODING = 1

    def __init__(self, datastore):
        self.__datastore = datastore
        self.__attachments_dir = datastore.extension_data_dir('com.cloudant.attachments')

    def add_attachment(self, attachment, rev):
        assert isinstance(attachment, PreparedAttachment)
        assert isinstance(rev, DocumentRevision)
        try:
            seq = rev.sequence
            filename = attachment.attachment.name
            sha1 = attachment.sha1
            type = attachment.attachment.type
            encoding = AttachmentManager.PLAIN_ENCODING
            length = attachment.attachment.size
            rev_pos = int(rev.revid.split('-')[0])
            values = {
                'sequence': seq,
                'filename': filename,
                'key': base64.b64encode(sha1),
                'type': type,
                'encoding': encoding,
                'length': length,
                'encoded_length': length,
                'revpos': rev_pos
            }

            self.__datastore.get_sql().delete('attachments', ' filename=? and sequence=? ', (filename, seq))
            result = self.__datastore.get_sql().insert('attachments', values)
            if result < 0:
                attachment.delete_tempfile()
                return False
            new_path = os.path.join(self.__attachments_dir, ''.join(map(lambda c: '%02x' % ord(c), sha1)))
            os.rename(attachment.tempfile, new_path)
        except:
            return False
        return True

    def update_attachments(self, rev, attachments):
        assert isinstance(rev, DocumentRevision)
        assert all(map(lambda x: isinstance(x, Attachment), attachments))
        prepped = map(lambda x: PreparedAttachment(x, self.__attachments_dir), attachments)
        self.__datastore.get_sql().begin_transaction()
        try:
            new_rev = self.__datastore.update(rev.docid, rev.revid, rev.body)
            ok = all(map(lambda x: self.add_attachment(x, new_rev), prepped))
            if ok:
                self.__datastore.get_sql().set_transaction_success()
                return new_rev
            return None
        finally:
            self.__datastore.get_sql().end_transaction()

    def get_attachment(self, rev, name):
        assert isinstance(rev, DocumentRevision)
        assert name is not None and len(name) > 0
        try:
            c = self.__datastore.get_sql().execute('SELECT sequence, filename, key, type, encoding,'
                                                   ' length, encoded_length, revpos FROM attachments'
                                                   ' WHERE filename = ? and sequence = ?', name, rev.sequence)
            row = c.fetchone()
            if row is not None:
                seq = long(row[0])
                key = base64.b64decode(str(row[2]))
                type = str(row[3])
                revpos = long(row[7])
                file = os.path.join(self.__attachments_dir, ''.join(map(lambda c: '%02x' % ord(c), key)))
                return SavedAttachment(name, revpos, seq, key, type, file)
            return None
        except:
            return None

    def all_attachments(self, rev):
        assert isinstance(rev, DocumentRevision)
        seq = rev.sequence
        c = self.__datastore.get_sql().execute('SELECT sequence, filename, key, type, encoding, length, encoded_length,'
                                               ' revpos'
                                               ' FROM attachments WHERE sequence = ?', (seq,))
        while True:
            row = c.fetchone()
            if row is None:
                break
            name = str(row[1])
            key = base64.b64decode(str(row[2]))
            type = str(row[3])
            revpos = long(row[7])
            file = os.path.join(self.__attachments_dir, ''.join(map(lambda c: '%02x' % ord(c), key)))
            yield SavedAttachment(name, revpos, seq, key, type, file)

    def remove_attachments(self, rev, names):
        assert isinstance(rev, DocumentRevision)
        assert names is not None and len(names) > 0
        deleted = self.__datastore.get_sql().delete('attachments', 'filename in (%s) AND sequence = ?' %
                                                                   ', '.join('?' * len(names)))
        if deleted:
            return self.__datastore.update(rev.docid, rev.revid, rev.body)
        else:
            return rev

    # todo purge_attachments
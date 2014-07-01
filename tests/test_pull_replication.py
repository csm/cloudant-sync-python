import requests
import logging
import os
import shutil
import sys
import tempfile
import time
import unittest

from cloudant.sync.datastore import Datastore
from cloudant.sync.replication.couchdb import CouchDB
from cloudant.sync.replication import *

username = os.environ.get('COUCHDB_USER', None)
password = os.environ.get('COUCHDB_PASSWORD', None)

couch_available = False
try:
    s = requests.session()
    if username is not None and password is not None:
        s.auth = (username, password)
    r = s.get('http://localhost:5984')
    r.raise_for_status()
    d = r.json()
    if d.get('couchdb') == 'Welcome':
        r = s.put('http://localhost:5984/cloudant_sync_test_couchdb')
        if r.status_code != 201 and r.status_code != 412:
            raise Exception('couch not available')
        r = s.delete('http://localhost:5984/cloudant_sync_test_couchdb')
        r.raise_for_status()
        couch_available = True
except:
    pass

l = logging.getLogger('cloudant.sync.replication')
l.setLevel(logging.DEBUG)
l.addHandler(logging.StreamHandler(sys.stdout))
l = logging.getLogger('cloudant.sync.datastore')
l.setLevel(logging.DEBUG)
l.addHandler(logging.StreamHandler(sys.stdout))


@unittest.skipIf(not couch_available, 'CouchDB must be available to run this test')
class TestPullReplication(unittest.TestCase):
    def setUp(self):
        self.__tempdir = os.path.join(tempfile.gettempdir(), 'cloudant_sync_test_pull_replication')
        if not os.path.exists(self.__tempdir):
            os.mkdir(self.__tempdir)
        self.__datastore = Datastore(self.__tempdir, 'cloudant_sync_test_pull_replication')
        self.__couch = CouchDB('localhost', 5984, database='cloudant_sync_test_pull_replication', username=username,
                               password=password, secure=False)
        self.__couch.create_database()

    def tearDown(self):
        self.__couch.delete_database()
        shutil.rmtree(self.__tempdir)

    def test_pull(self):
        # First, add some documents to CouchDB, so we can pull them down.
        self.__couch.create({'_id': 'test_doc1', 'foo': 'bar'})
        self.__couch.create({'_id': 'test_doc2', 'foo': 'baz'})
        replication = PullReplication('http://localhost:5984/cloudant_sync_test_pull_replication',
                                      target=self.__datastore, username=username, password=password)
        rep = replicator(replication)
        rep.start()
        n = 100000000000
        print rep.state
        while rep.state == State.STARTED:
            time.sleep(0.1)
            n -= 1
            if n < 0:
                self.fail('timed out waiting for replication to finish')
        self.assertEqual(State.COMPLETE, rep.state)
        doc = self.__datastore.get('test_doc1')
        self.assertIsNotNone(doc)
        self.assertEqual('bar', doc.body.to_dict().get('foo'))
        doc = self.__datastore.get('test_doc2')
        self.assertIsNotNone(doc)
        self.assertEqual('baz', doc.body.to_dict().get('foo'))

if __name__ == '__main__':
    unittest.main()
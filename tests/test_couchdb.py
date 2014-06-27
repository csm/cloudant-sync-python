import os
import requests
import unittest

from cloudant.sync.replication.couchdb import CouchDB

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


@unittest.skipIf(not couch_available, 'only run this test of CouchDB is running locally')
class TestCouchdb(unittest.TestCase):
    def setUp(self):
        r = s.put('http://localhost:5984/cloudant_sync_test_couchdb')
        if r.status_code != 201 and r.status_code != 412:
            raise Exception('failed to create test DB')
        self.couchdb = CouchDB('localhost', 5984, 'cloudant_sync_test_couchdb', username, password)

    def tearDown(self):
        r = s.delete('http://localhost:5984/cloudant_sync_test_couchdb')
        r.raise_for_status()

    def test_checkpoint(self):
        cp = self.couchdb.get_checkpoint("foo")
        self.assertIsNone(cp)
        self.couchdb.set_checkpoint("foo", 123)
        cp = self.couchdb.get_checkpoint("foo")
        self.assertEqual(123, cp)

    def test_changes(self):
        changes = self.couchdb.changes()
        self.assertTrue('last_seq' in changes)
        self.assertTrue('results' in changes)

    def test_create(self):
        obj = dict(foo="bar")
        result = self.couchdb.create(obj)
        obj2 = self.couchdb.get(result.get('id'))
        self.assertEqual(obj['foo'], obj2['foo'])

    def test_create_update(self):
        obj1 = dict(foo="bar")
        result1 = self.couchdb.create(obj1)
        obj2 = dict(foo="baz", _rev=result1.get('rev'))
        result2 = self.couchdb.update(result1.get('id'), obj2)
        obj3 = self.couchdb.get(result2.get('id'))
        self.assertEqual(obj2['foo'], obj3['foo'])

    def test_create_delete(self):
        obj1 = dict(foo='bar')
        result1 = self.couchdb.create(obj1)
        result2 = self.couchdb.delete(result1['id'], result1['rev'])
        obj2 = self.couchdb.get(result1['id'])
        self.assertIsNone(obj2)
import unittest
import cloudant.sync.datastore


class TestDatastore(unittest.TestCase):
    def setUp(self):
        self.__datastore = cloudant.sync.datastore.Datastore(None, 'test', in_memory=True)

    def tearDown(self):
        self.__datastore = None

    def test_put_get(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'test', 'value': 1234})
        rev1 = self.__datastore.create(doc1, 'doc1')
        rev2 = self.__datastore.get('doc1', rev1.revid)
        self.assertIsNotNone(rev2)
        self.assertEqual(rev2.docid, 'doc1')
        doc2 = rev2.body
        self.assertEqual(doc1.to_dict(), doc2.to_dict())

    def test_put_get2(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'test', 'value': 1234})
        self.__datastore.create(doc1, 'doc1')
        rev2 = self.__datastore.get('doc1')
        self.assertIsNotNone(rev2)
        self.assertEqual(rev2.docid, 'doc1')
        doc2 = rev2.body
        self.assertEqual(doc1.to_dict(), doc2.to_dict())

    def test_put_get_mod(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'test', 'value': 1234})
        self.__datastore.create(doc1, 'doc1')
        rev = self.__datastore.get('doc1')
        doc2 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'foo', 'value': 4321})
        self.__datastore.update('doc1', rev.revid, doc2)
        rev2 = self.__datastore.get('doc1')
        doc3 = rev2.body
        self.assertEqual(doc2.to_dict(), doc3.to_dict())

    def test_conflict_create(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'version': 1})
        self.__datastore.create(doc1, 'doc')
        doc2 = cloudant.sync.datastore.DocumentBody(dict_value={'version': 2})
        try:
            self.__datastore.create(doc2, 'doc')
            self.fail('was able to create existing doc')
        except cloudant.sync.datastore.ConflictError:
            pass
        rev = self.__datastore.get('doc')
        doc3 = rev.body
        self.assertEqual(doc1.to_dict(), doc3.to_dict())

    def test_conflict_update(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'a'})
        rev1 = self.__datastore.create(doc1, 'doc')
        doc2 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'b'})
        rev2 = self.__datastore.update('doc', rev1.revid, doc2)
        doc3 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'c'})
        try:
            self.__datastore.create(doc3, 'doc')
            self.fail('create of existing')
        except cloudant.sync.datastore.ConflictError:
            pass
        try:
            self.__datastore.update('doc', rev1.revid, doc3)
            self.fail('update of wrong version')
        except cloudant.sync.datastore.ConflictError:
            pass
        rev = self.__datastore.get('doc')
        self.assertEqual(doc2.to_dict(), rev.body.to_dict())

    def test_delete(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'foo': 'bar'})
        rev1 = self.__datastore.create(doc1, 'doc')
        self.__datastore.delete('doc', rev1.revid)
        rev2 = self.__datastore.get('doc')
        self.assertTrue(rev2.deleted)

    def test_delete_create(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'foo': 'bar'})
        rev1 = self.__datastore.create(doc1, 'doc')
        self.__datastore.delete('doc', rev1.revid)
        rev2 = self.__datastore.get('doc')
        self.assertTrue(rev2.deleted)
        doc2 = cloudant.sync.datastore.DocumentBody(dict_value={'foo': 'baz'})

        # TODO this can't be right: if we query a deleted document, we get the deleted
        # rev back; if we overwrite a deleted document, we have to 'update' it, we
        # can't just 'create' it again. This looks like its how the Java API does it,
        # though, unless I'm missing something...

        self.__datastore.update('doc', rev2.revid, doc2)
        rev4 = self.__datastore.get('doc')
        self.assertEqual(doc2.to_dict(), rev4.body.to_dict())

if __name__ == '__main__':
    unittest.main()
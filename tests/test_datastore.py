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
        assert rev2 is not None
        assert rev2.docid == 'doc1'
        doc2 = rev2.body
        assert doc1.to_dict()['name'] == doc2.to_dict()['name']
        assert doc1.to_dict()['value'] == doc2.to_dict()['value']

    def test_put_get2(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'test', 'value': 1234})
        self.__datastore.create(doc1, 'doc1')
        rev2 = self.__datastore.get('doc1')
        assert rev2 is not None
        assert rev2.docid == 'doc1'
        doc2 = rev2.body
        assert doc1.to_dict()['name'] == doc2.to_dict()['name']
        assert doc1.to_dict()['value'] == doc2.to_dict()['value']

    def test_put_get_mod(self):
        doc1 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'test', 'value': 1234})
        self.__datastore.create(doc1, 'doc1')
        rev = self.__datastore.get('doc1')
        doc2 = cloudant.sync.datastore.DocumentBody(dict_value={'name': 'foo', 'value': 4321})
        self.__datastore.update('doc1', rev.revid, doc2)
        rev2 = self.__datastore.get('doc1')
        doc3 = rev2.body
        assert doc2.to_dict()['name'] == doc3.to_dict()['name']
        assert doc2.to_dict()['value'] == doc3.to_dict()['value']

if __name__ == '__main__':
    unittest.main()
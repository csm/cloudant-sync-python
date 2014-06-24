import os
import tempfile
import unittest
from cloudant.sync.datastore import SavedAttachment, UnsavedBytesAttachment


class TestUnsavedBytesAttachment(unittest.TestCase):
    def test1(self):
        data = 'a' * 256
        att = UnsavedBytesAttachment('test1', 'text/plain', data)
        data2 = att.get_data().read()
        assert data == data2


class TestSavedBytesAttachment(unittest.TestCase):
    def setUp(self):
        f, self.temp_path = tempfile.mkstemp()
        os.write(f, 'a' * 256)
        os.close(f)

    def tearDown(self):
        os.unlink(self.temp_path)

    def test_not_exist(self):
        p = '/this_path_should_not_exist_anywhere.txt'
        assert not os.path.exists(p)
        try:
            att = SavedAttachment('test_not_exist', 'text/plain', 0, 0, None, p)
            self.fail('should not be able to create this attachment')
        except OSError:
            pass

    def test_exists(self):
        att = SavedAttachment('test_exists', 'text/plain', 0, 0, '\x9cxQ*\xd1P\xc8\xb5\xd8\x91\x83\x95\xad\x0eQi9}+b', self.temp_path)
        data = att.get_data().read()
        assert ('a' * 256) == data

if __name__ == '__main__':
    unittest.main()
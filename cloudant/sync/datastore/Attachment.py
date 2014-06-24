import os
import StringIO


class Attachment(object):
    def __init__(self, name=None, content_type=None, size=0):
        self.__name = name
        self.__content_type = content_type
        self.__size = size

    @property
    def name(self):
        return self.__name

    @name.setter
    def set_name(self, name):
        self.__name = name

    @property
    def content_type(self):
        return self.__content_type

    @content_type.setter
    def set_content_type(self, content_type):
        self.__content_type = content_type

    @property
    def size(self):
        return self.__size

    @size.setter
    def set_size(self, size):
        self.__size = size

    def get_data(self):
        raise NotImplementedError('this class is abstract, return a file-like object to implement this')


class SavedAttachment(Attachment):
    def __init__(self, name, revpos, seq, key, content_type, path):
        Attachment.__init__(self, name, content_type, os.path.getsize(path))
        self.__path = path
        self.__revpos = revpos
        self.__seq = seq
        self.__key = key

    @property
    def revpos(self):
        return self.__revpos

    @property
    def seq(self):
        return self.__seq

    @property
    def key(self):
        return self.__key

    def get_data(self):
        return open(self.__path, 'rb')


class UnsavedBytesAttachment(Attachment):
    def __init__(self, name, content_type, data):
        Attachment.__init__(self, name, content_type, len(data))
        self.__bytes = data

    def get_data(self):
        return StringIO.StringIO(self.__bytes)
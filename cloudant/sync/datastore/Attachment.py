import os
import StringIO


class Attachment(object):
    def __init__(self, name=None, type=None, size=0):
        self.__name = name
        self.__type = type
        self.__size = size

    @property
    def name(self):
        return self.__name

    @name.setter
    def set_name(self, name):
        self.__name = name

    @property
    def type(self):
        return self.__type

    @type.setter
    def set_type(self, type):
        self.__type = type

    @property
    def size(self):
        return self.__size

    @size.setter
    def set_size(self, size):
        self.__size = size

    def get_data(self):
        raise NotImplementedError('this class is abstract, return a file-like object to implement this')


class SavedAttachment(Attachment):
    def __init__(self, name, revpos, seq, key, type, file):
        Attachment.__init__(name, type, os.path.getsize(file))
        self.__file = file
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
        return open(self.__file, 'rb')


class UnsavedBytesAttachment(Attachment):
    def __init__(self, name, type, data):
        Attachment.__init__(name, type, len(data))
        self.__bytes = data

    def get_data(self):
        return StringIO.StringIO(self.__bytes)
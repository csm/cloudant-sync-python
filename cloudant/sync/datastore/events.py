class DocumentModified(object):
    def __init__(self, old_rev, new_rev):
        self.__oldrev = old_rev
        self.__newrev = new_rev

    @property
    def old_rev(self):
        return self.__oldrev

    @property
    def new_rev(self):
        return self.__newrev


class DocumentCreated(DocumentModified):
    def __init__(self, new_rev):
        DocumentModified.__init__(self, None, new_rev)


class DocumentUpdated(DocumentModified):
    def __init__(self, old_rev, new_rev):
        DocumentModified.__init__(self, old_rev, new_rev)


class DocumentDeleted(DocumentModified):
    def __init__(self, old_rev, new_rev):
        DocumentModified.__init__(self, old_rev, new_rev)
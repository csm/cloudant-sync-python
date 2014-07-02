from cloudant.sync.datastore import Datastore
import urllib
import urlparse


class ReplicationFilter(object):
    def __init__(self, name, parameters={}):
        assert isinstance(name, basestring)
        assert isinstance(parameters, dict)
        self.__name = name
        self.__parameters = parameters

    @property
    def name(self):
        return self.__name

    @property
    def parameters(self):
        return self.__parameters

    def query_string(self):
        parts = ['filter=%s' % urllib.quote_plus(self.name)]
        for key, value in self.parameters:
            parts.append('%s=%s' % (urllib.quote_plus(key), urllib.quote_plus(value)))
        parts.sort()
        return '&'.join(parts)

    def __repr__(self):
        return 'ReplicationFilter(name=%r, parameters=%r)' % (self.name, self.parameters)


class Replication(object):
    def __init__(self, username=None, password=None):
        assert username is None or isinstance(username, basestring)
        assert password is None or isinstance(password, basestring)
        self.__username = username
        self.__password = password

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, username):
        assert username is None or isinstance(username, basestring)
        self.__username = username

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, password):
        assert password is None or isinstance(password, basestring)

    def validate(self):
        pass


class PullReplication(Replication):
    def __init__(self, uri=None, target=None, filter_=None, username=None, password=None):
        Replication.__init__(self, username, password)
        assert uri is None or isinstance(uri, basestring)
        assert target is None or isinstance(target, Datastore)
        assert filter_ is None or isinstance(filter_, ReplicationFilter)
        self.__uri = uri
        self.__target = target
        self.__filter = filter_

    @property
    def uri(self):
        return self.__uri

    @uri.setter
    def uri(self, uri):
        assert uri is None or isinstance(uri, str) or isinstance(uri, unicode)
        self.__uri = uri

    @property
    def target(self):
        return self.__target

    @target.setter
    def target(self, target):
        assert target is None or isinstance(target, Datastore)
        self.__target = target

    @property
    def filter(self):
        return self.__filter

    @filter.setter
    def filter(self, filter_):
        assert filter_ is None or isinstance(filter_, ReplicationFilter)
        self.__filter = filter_

    def validate(self):
        assert self.uri is not None
        assert self.target is not None
        urlparse.urlparse(self.uri, allow_fragments=False)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'PullReplication(uri=%r, target=%r, filter=%r, username=%r)' % (self.uri, self.target.name, self.filter,
                                                                               self.username)


class PushReplication(Replication):
    def __init__(self, uri=None, source=None, username=None, password=None):
        Replication.__init__(self, username, password)
        assert uri is None or isinstance(uri, basestring)
        assert source is None or isinstance(source, Datastore)
        self.__uri = uri
        self.__source = source

    @property
    def source(self):
        return self.__source

    @source.setter
    def source(self, source):
        assert source is None or isinstance(source, Datastore)
        self.__source = source

    @property
    def uri(self):
        return self.__uri

    @uri.setter
    def uri(self, uri):
        assert uri is None or isinstance(uri, basestring)
        self.__uri = uri

    def validate(self):
        assert self.uri is not None
        assert self.source is not None
        urlparse.urlparse(self.uri, allow_fragments=False)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'PushReplication(uri=%r, source=%s, username=%s)' % (self.uri, self.source.name, self.username)

def replicator(replication, on_completed=None, on_errored=None, config=None, executor=None):
    if isinstance(replication, PushReplication):
        from .replicators import PushReplicator
        return PushReplicator(replication, on_completed=on_completed, on_errored=on_errored, config=config,
                              executor=executor)
    if isinstance(replication, PullReplication):
        from .replicators import PullReplicator
        return PullReplicator(replication, on_completed=on_completed, on_errored=on_errored, config=config,
                              executor=executor)
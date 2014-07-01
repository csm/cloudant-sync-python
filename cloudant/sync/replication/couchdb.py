import email
import json
import requests
import urllib

from cloudant.sync.datastore import DocumentRevision


class CouchDB(object):
    """
    A utility class for accessing a CouchDB database.
    """

    def __init__(self, host, port, database, username, password, secure=False):
        self.__url = '%s://%s:%d/%s' % ('https' if secure else 'http', host, port, database)
        self.__session = requests.session()
        if username is not None and password is not None:
            self.__session.auth = (username, password)
        self.__database = database

    def exists(self):
        r = self.__session.get(self.__url)
        return r.status_code == 200

    @property
    def database(self):
        return self.__database

    def create_database(self):
        """
        Create this database, or return an error if it already exists.
        """
        self.__session.put(self.__url).raise_for_status()

    def delete_database(self):
        self.__session.delete(self.__url).raise_for_status()

    def get_checkpoint(self, checkpoint_id):
        try:
            r = self.__session.get('%s/_local/%s' % (self.__url, urllib.quote_plus(checkpoint_id)))
            r.raise_for_status()
            return r.json().get('lastSequence')
        except IOError, e:
            return None

    def set_checkpoint(self, checkpoint_id, sequence):
        doc = {'lastSequence': sequence}
        r = self.__session.get('%s/_local/%s' % (self.__url, urllib.quote_plus(checkpoint_id)))
        if r.status_code == 200:
            doc['_rev'] = r.json().get('_rev')
        r = self.__session.put('%s/_local/%s' % (self.__url, urllib.quote_plus(checkpoint_id)), data=json.dumps(doc),
                               headers={'Content-type': 'application/json'})
        r.raise_for_status()

    def changes(self, limit=100, last_seq=None, filter_=None):
        params = dict(limit=limit)
        if last_seq is not None:
            params['since'] = last_seq
        if filter_ is not None:
            params['filter'] = filter_.name
            for k,v in filter_.parameters.items():
                if k != 'since' and k != 'limit':
                    params[k] = v
        r = self.__session.get('%s/_changes' % self.__url, params=params)
        r.raise_for_status()
        return r.json()

    def get_revs(self, docid, revisions):
        params = dict(revs='true', attachments='true', open_revs=json.dumps(revisions))
        r = self.__session.get('%s/%s' % (self.__url, urllib.quote_plus(docid)), params=params)
        r.raise_for_status()
        # CouchDB may return multipart/mixed content here. Handle that.
        if r.headers['content-type'].startswith('multipart/mixed'):
            # there's got to be a better way...
            msg = email.message_from_string('Content-Type: %s\r\n\r\n%s' % (r.headers['content-type'], r.content))
            for item in msg.get_payload():
                if item.get('content-type') == 'application/json':
                    return json.loads(item.get_payload())
        return r.json()

    def create(self, obj):
        r = self.__session.post(self.__url, data=json.dumps(obj), headers={'Content-type': 'application/json'})
        r.raise_for_status()
        return r.json()

    def update(self, docid, obj):
        r = self.__session.put('%s/%s' % (self.__url, urllib.quote_plus(docid)),
                               headers={'Content-type': 'application/json'}, data=json.dumps(obj))
        r.raise_for_status()
        return r.json()

    def get(self, docid):
        r = self.__session.get('%s/%s' % (self.__url, urllib.quote_plus(docid)))
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def delete(self, docid, revid):
        r = self.__session.delete('%s/%s' % (self.__url, urllib.quote_plus(docid)), params=dict(rev=revid))
        r.raise_for_status()
        return r.json()

    def bulk(self, revs):
        if len(revs) == 0 or not all(map(lambda e: isinstance(e, DocumentRevision), revs)):
            raise ValueError('revs must be a list of DocumentRevisions')
        objs = map(lambda e: e.to_dict(), revs)
        r = self.__session.post('%s/_bulk_docs' % self.__url, data=json.dumps({'docs': objs}))
        r.raise_for_status()
        return r.json()

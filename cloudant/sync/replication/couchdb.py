import json
import requests
import urllib


class CouchDB(object):
    def __init__(self, host, port, database, username, password, secure=False):
        self.__url = '%s://%s:%d/%s' % ('https' if secure else 'http', host, port, database)
        self.__session = requests.session()
        self.__session.auth = (username, password)
        self.__database = database

    def exists(self):
        r = self.__session.get(self.__url)
        return r.status_code == 200

    @property
    def database(self):
        return self.__database

    def get_checkpoint(self, checkpoint_id):
        try:
            r = self.__session.get('%s/_local/%s' % (self.__url, urllib.quote_plus(checkpoint_id)))
            r.raise_for_status()
            return r.json().get('lastSequence')
        except Exception, e:
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
        raise NotImplementedError()

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
        r.raise_for_status()
        return r.json()

    def delete(self, docid, revid):
        r = self.__session.delete('%s/%s' % (self.__url, urllib.quote_plus(docid)), params=dict(rev=revid))
        r.raise_for_status()
        return r.json()


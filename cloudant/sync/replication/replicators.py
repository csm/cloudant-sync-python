import base64
from collections import defaultdict
import concurrent.futures
from itertools import izip_longest
import json
import logging
from threading import Thread, Condition
import time
import traceback
import urlparse

from couchdb import CouchDB
from state import State, IllegalStateError
from replications import PushReplication, PullReplication
from cloudant.sync.datastore import DocumentRevision, DocumentBody


class DatabaseNotFoundError(IOError):
    pass


class Replicator(object):
    def __init__(self, replication, on_completed=None, on_errored=None):
        self._replication = replication
        self._state = State.PENDING
        self._cancel = False
        self.__thread = None
        self.__cancel_lock = Condition()
        self.__on_completed = on_completed
        self.__on_errored = on_errored

    def start(self):
        if self._state == State.STARTED:
            return
        if self._state == State.STOPPING:
            raise IllegalStateError('replicator is stopping')
        if self._state == State.PENDING or self._state == State.COMPLETE or self._state == State.STOPPED:
            self._cancel = False
            t = Thread(target=self.__do_run)
            t.setDaemon(True)
            t.start()
            self._state = State.STARTED
            self.__thread = t

    def stop(self):
        if self._state == State.PENDING:
            self._state = State.STOPPED
        if self._state == State.STARTED:
            self._state = State.STOPPING
            self.__cancel_lock.acquire()
            try:
                self._cancel = True
                self.__cancel_lock.notifyAll()
            finally:
                self.__cancel_lock.release()
            self.__thread.join()
            self._state = State.STOPPED

    @property
    def state(self):
        return self._state

    @property
    def replication(self):
        return self._replication

    def __do_run(self):
        try:
            _logger().info('Replication %r starting...', self)
            self._replicate()
            self._state = State.COMPLETE
            if self.__on_completed is not None:
                try:
                    self.__on_completed(self)
                except:
                    pass
        except Exception, e:
            if self.__on_errored is not None:
                try:
                    self.__on_errored(self, e)
                except:
                    pass
            _logger().warning('replication %r errored: %s', self, e)
            _logger().warning('%s', traceback.format_exc())
            self._state = State.ERROR

    def _replicate(self):
        pass


class PullConfiguration(object):
    def __init__(self, changes_per_batch=1000, max_batches=100, insert_batch_size=10):
        self.changes_per_batch = changes_per_batch
        self.max_batches = max_batches
        self.insert_batch_size = insert_batch_size

    def __repr__(self):
        return '%s(changes_per_batch=%r, max_batches=%r, insert_batch_size=%r)' % (PullConfiguration.__name__,
                                                                                   self.changes_per_batch,
                                                                                   self.max_batches,
                                                                                   self.insert_batch_size)


def open_revisions(changes, start, end):
    assert start >= 0
    assert end > start
    assert end <= len(changes.get('results', []))
    d = defaultdict(list)
    for row in changes.get('results', [])[start:end]:
        for change in row.get('changes', []):
            d[row.get('id')].append(change.get('rev'))
    return d


def _logger():
    return logging.getLogger('cloudant.sync.replication')


class PullReplicator(Replicator):
    def __init__(self, replication, on_completed=None, on_errored=None, executor=None, config=None):
        super(PullReplicator, self).__init__(replication, on_completed, on_errored)
        assert isinstance(replication, PullReplication)
        self.__target = replication.target
        u = urlparse.urlparse(replication.uri)
        self.__couch = CouchDB(u.hostname, u.port, u.path, replication.username, replication.password,
                               u.scheme == 'https')
        self.__doc_counter = 0
        self.__batch_counter = 0
        if config is None:
            self.config = PullConfiguration()
        else:
            if not isinstance(config, PullConfiguration):
                raise ValueError('config must be a PullConfiguration')
            self.config = config
        if executor is None:
            self.__executor = concurrent.futures.ThreadPoolExecutor(4)
        else:
            if not isinstance(executor, concurrent.futures.Executor):
                raise ValueError('executor argument must be a concurrent.futures.Executor')
            self.__executor = executor

    def __replication_id(self):
        if self.replication.filter is None:
            return self.__couch.database
        else:
            return '%s?%s' % (self.__couch.database, self.replication.filter.query_string())

    def _replicate(self):
        if self._cancel:
            return
        if not self.__couch.exists():
            raise DatabaseNotFoundError('database not found: ' + self._replication.uri)
        start_time = time.time()
        self.__doc_counter = 0
        self.__batch_counter = 1
        while self.__batch_counter < self.config.max_batches:
            if self._cancel:
                return
            last_checkpoint = None
            doc = self.__target.get_local('_local/' + self.__replication_id())
            if doc is not None and not doc.deleted:
                last_checkpoint = doc.body.to_dict().get('lastSequence')
            _logger().info('fetching changes batch, limit=%s, last_seq=%s, filter=%r',
                                 self.config.changes_per_batch, last_checkpoint,
                                 self.replication.filter)
            changes = self.__couch.changes(limit=self.config.changes_per_batch, last_seq=last_checkpoint,
                                           filter_=self.replication.filter)
            _logger().debug('got changes: %r', changes)
            if len(changes.get('results', [])) > 0:
                batch_processed = self.__handle_batch(changes)
                self.__doc_counter += batch_processed
            if len(changes.get('results', [])) < self.config.changes_per_batch:
                break
            self.__batch_counter += 1
        _logger().info('processed %d documents in %s', self.__doc_counter, time.time() - start_time)

    def __handle_batch(self, changes):
        open_revs = open_revisions(changes, 0, len(changes.get('results', [])))
        missing_revs = self.__target.revs_diff(open_revs)
        _logger().debug('missing_revs: %r', missing_revs)
        changes_processed = 0
        ids = list(missing_revs.keys())
        batches = map(lambda b: filter(lambda e: e is not None, b), izip_longest(*[iter(ids)]*self.config.insert_batch_size))
        _logger().debug('handle_batches, batch: %r', batches)
        for batch in batches:
            futures = map(lambda _id: self.__executor.submit(lambda: self.__couch.get_revs(_id, missing_revs[_id])), batch)
            for future in futures:
                assert isinstance(future, concurrent.futures.Future)
                rev = future.result()
                _logger().debug('handling rev: %r', rev)
                if self._cancel:
                    break
                body = DocumentBody(bytes_value='{}')
                if rev.get('_deleted') is not True:
                    body = DocumentBody(dict_value={key: rev[key] for key in rev if key[0] != '_'})
                doc = DocumentRevision(rev.get('_id'), rev.get('_rev'), body)
                rev_history = ['%d-%s' % (rev['_revisions']['start'] - i, rev['_revisions']['ids'][i])
                               for i in range(0, len(rev['_revisions']['ids']))]
                _logger().debug('rev_history: %r', rev_history)
                self.__target.force_insert(doc, rev_history, rev.get('_attachments'))
                changes_processed += 1
        if not self._cancel:
            checkpoint_id = '_local/' + self.__replication_id()
            doc = self.__target.get_local(checkpoint_id)
            body = DocumentBody(dict_value=dict(lastSequence=changes.get('last_seq')))
            if doc is None:
                self.__target.create_local(body)
            else:
                self.__target.update_local(checkpoint_id, doc.revid, body)
        return changes_processed

    def __repr__(self):
        return 'PullReplicator(%r)' % self.replication


class PushConfiguration(object):
    def __init__(self, changes_per_batch=1000, max_batches=100, insert_batch_size=10):
        self.changes_per_batch = changes_per_batch
        self.max_batches = max_batches
        self.insert_batch_size = insert_batch_size

    def __repr__(self):
        return '%s(changes_per_batch=%r, max_batches=%r, insert_batch_size=%r)' % (PushConfiguration.__name__,
                                                                                   self.changes_per_batch,
                                                                                   self.max_batches,
                                                                                   self.insert_batch_size)


class PushReplicator(Replicator):
    def __init__(self, replication, on_completed=None, on_errored=None, executor=None, config=None):
        super(PushReplicator, self).__init__(replication, on_completed, on_errored)
        if not isinstance(replication, PushReplication):
            raise ValueError('replication must be a PushReplication')
        self.__replication = replication
        u = urlparse.urlparse(replication.uri)
        self.__target = CouchDB(u.hostname, u.port, u.path, replication.username, replication.password,
                                u.scheme == 'https')
        if config is None:
            self.config = PushConfiguration()
        else:
            if not isinstance(config, PushConfiguration):
                raise ValueError('config must be a PushConfiguration')
            self.config = config
        if executor is None:
            self.__executor = concurrent.futures.ThreadPoolExecutor(4)
        else:
            if not isinstance(executor, concurrent.futures.Executor):
                raise ValueError('executor must be a concurrent.futures.Executor')
            self.__executor = executor
        self.__doc_count = 0
        self.__batch_count = 0

    def _replicate(self):
        _logger().info('Replication %r started', self)
        start_time = time.time()
        if self._cancel:
            return
        if not self.__target.exists():
            raise DatabaseNotFoundError('database %s not found' % self.__target.database)
        self.__doc_count = 0
        self.__batch_count = 1
        while self.__batch_count < self.config.max_batches:
            if self._cancel:
                return
            _logger().info('Batch %d started (completed %d changes so far)', self.__batch_count, self.__doc_count)
            batch_start_time = time.time()
            last_seq = self.__target.get_checkpoint(self.__replication.source.get_public_id()) or 0
            changes = self.__replication.source.changes(since=last_seq, limit=self.config.changes_per_batch)
            changes_processed = 0
            _logger().debug('last_seq: %d got changes: %r', last_seq, changes)
            if len(changes) > 0:
                changes_processed = self.__process_batch(changes)
                self.__doc_count += changes_processed
            batch_end_time = time.time()
            _logger().info('Batch %d completed in %s seconds (processed %d changes)', self.__batch_count,
                           batch_end_time - batch_start_time, changes_processed)
            if len(changes) == 0:
                break
            self.__batch_count += 1
        end_time = time.time()
        _logger().info('Push completed in %s seconds (%s total changes processed)', end_time - start_time,
                       self.__doc_count)

    def __process_batch(self, changes):
        num = 0
        batches = map(lambda b: filter(lambda e: e is not None, b),
                      izip_longest(*[iter(changes.results)]*self.config.insert_batch_size))
        for batch in batches:
            if self._cancel:
                break
            all_trees = reduce(lambda a, b: a.update(b) or a,
                               map(lambda doc: {doc.docid: self.__replication.source.get_revisions(doc.docid)}, batch), {})
            open_revs = PushReplicator.__open_revisions(all_trees)
            missing_revs = self.__target.revs_diff(open_revs)
            missing_json = self.__missing_to_json(all_trees, missing_revs)
            if not self._cancel:
                self.__target.bulk_serialized(missing_json)
                num += len(missing_json)
        if not self._cancel:
            self.__target.set_checkpoint(self.__replication.source.get_public_id(), changes.last_seq)
        return num

    def __missing_to_json(self, all_trees, missing):
        docs = []
        for doc_id, missing_revs in missing.iteritems():
            tree = all_trees.get(doc_id)
            for rev in missing_revs:
                seq = tree.lookup(doc_id, rev).sequence
                path = map(lambda e: e.data, tree.path_for_node(seq))
                doc_rev = path[0]
                atts = self.__replication.source.attachments_for_revision(doc_rev)
                cur = doc_rev.to_dict()
                revpos = long(doc_rev.revid.split('-')[0])
                att_list = {}
                for att in atts:
                    att_dict = {}
                    if att.revpos < revpos:
                        att_dict['stub'] = True
                    else:
                        with att.get_data() as f:
                            data = base64.b64encode(f.read())
                        att_dict['data'] = data
                    att_dict['content_type'] = att.content_type
                    att_dict['revpos'] = att.revpos
                    att_list[att.name] = att_dict
                cur['_attachments'] = att_list
                revisions = {'start': revpos, 'ids': [s.revid.split('-')[1] for s in path]}
                cur['_revisions'] = revisions
                docs.append(json.dumps(cur))
        return docs

    @staticmethod
    def __open_revisions(all_trees):
        open_revs = defaultdict(list)
        for key, value in all_trees.iteritems():
            open_revs[key] += value.leaf_revids()
        return open_revs

    def __str__(self):
        return 'PushReplicator(%s)' % self.replication

    def __repr__(self):
        return 'PushReplicator(%r)' % self.replication
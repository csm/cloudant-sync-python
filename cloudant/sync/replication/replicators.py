import logging
from threading import Thread, Condition

from state import State, IllegalStateError


class Replicator(object):
    def __init__(self, replication, on_completed=None, on_errored=None):
        self.__replication = replication
        self.__state = State.PENDING
        self.__cancel = False
        self.__thread = None
        self.__cancel_lock = Condition()
        self.__on_completed = on_completed
        self.__on_errored = on_errored

    def start(self):
        if self.__state == State.STARTED:
            return
        if self.__state == State.STOPPING:
            raise IllegalStateError('replicator is stopping')
        if self.__state == State.PENDING or self.__state == State.COMPLETE or self.__state == State.STOPPED:
            self.__cancel = False
            t = Thread(target=self.__do_run())
            t.setDaemon(True)
            t.start()
            self.__state = State.STARTED
            self.__thread = t

    def stop(self):
        if self.__state == State.PENDING:
            self.__state = State.STOPPED
        if self.__state == State.STARTED:
            self.__state = State.STOPPING
            self.__cancel_lock.acquire()
            try:
                self.__cancel = True
                self.__cancel_lock.notifyAll()
            finally:
                self.__cancel_lock.release()
            self.__thread.join()
            self.__state = State.STOPPED

    @property
    def state(self):
        return self.__state

    @property
    def replication(self):
        return self.__replication

    def __do_run(self):
        try:
            logging.info('Replication %r starting...', self)
            self.__replicate()
            self.__state = State.COMPLETE
            if self.__on_completed is not None:
                try:
                    self.__on_completed(self)
                except:
                    pass
        except Exception, e:
            logging.warning('replication %r errored: %s', self, e)
            self.__state = State.ERROR

    def __replicate(self):
        pass


class PullReplicator(Replicator):
    def __init__(self, replication, on_completed=None, on_errored=None):
        super(PullReplicator, self).__init__(replication, on_completed, on_errored)
        self.__replication = replication

    def __replicate(self):
        pass

    def __repr__(self):
        return 'PullReplicator(%r)' % self.replication


class PushReplicator(Replicator):
    def __init__(self, replication, on_completed=None, on_errored=None):
        super(PushReplicator, self).__init__(replication, on_completed, on_errored)
        self.__replication = replication

    def __replicate(self):
        pass

    def __repr__(self):
        return 'PushReplicator(%r)' % self.replication
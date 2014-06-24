from state import State

class Replicator(object):
    pass


class PullReplicator(Replicator):
    def __init__(self, replication):
        self.__replication = replication
        self.__state = State.PENDING

    def __replicate(self):
        pass


class PushReplicator(Replicator):
    def __init__(self, replication):
        self.__replication = replication
        self.__state = State.PENDING

    def __replicate(self):
        pass
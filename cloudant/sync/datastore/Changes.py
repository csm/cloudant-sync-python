import copy


class Changes(object):
    def __init__(self, last_seq, revs):
        self.__last_seq = last_seq
        self.__revs = revs

    @property
    def last_seq(self):
        return self.__last_seq

    @property
    def results(self):
        return copy.copy(self.__revs)

    def __len__(self):
        return len(self.__revs)

    @property
    def ids(self):
        return map(lambda rev: rev.docid, self.__revs)

    def __repr__(self):
        return '%s(last_seq=%d, revs=%r)' % (Changes.__name__, self.last_seq, self.results)
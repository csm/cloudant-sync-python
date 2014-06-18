import copy
from .DocumentRevision import DocumentRevision


class DocumentRevisionNode(object):
    def __init__(self, data):
        self.__data = data
        self.__depth = 0
        self.__children = []

    def __iter__(self):
        return iter(self.__children)

    def __len__(self):
        return len(self.__children)

    def add_child(self, other):
        if not isinstance(other, DocumentRevisionNode):
            raise ValueError('should have a child node')
        self.__children.append(other)
        other.__depth = self.depth + 1
        other.__children_depth()

    def __children_depth(self):
        for child in self:
            child.__depth = self.__depth + 1
            child.__children_depth()

    @property
    def data(self): return self.__data

    @data.setter
    def data(self, data): self.__data = data

    @property
    def depth(self): return self.__depth


class DocumentRevisionTree(object):
    def __init__(self, node=None):
        self.__roots = {}
        self.__leafs = []
        self.__sequence_map = {}
        if node is not None:
            if not isinstance(node, DocumentRevision):
                raise ValueError('node must be a DocumentRevision')
            self.__add_root(node)

    def add(self, rev):
        if not isinstance(rev, DocumentRevision):
            raise ValueError('node must be a DocumentRevision')
        if rev.parent <= 0:
            self.__add_root(rev)
        else:
            self.__add_node(rev)

    def __add_root(self, root):
        if root.parent > 0:
            raise ValueError('The added root DocumentRevision must be a valid root revision')
        root_node = DocumentRevisionNode(root)
        self.__roots[root.sequence] = root_node
        self.__leafs.append(root_node)
        self.__sequence_map[root.sequence] = root_node

    def __add_node(self, rev):
        parent_seq = rev.parent
        if not parent_seq in self.__sequence_map:
            raise ValueError('The given revision\'s parent must be in the tree already')
        parent = self.__sequence_map[parent_seq]
        node = DocumentRevisionNode(rev)
        parent.add_child(node)
        try:
            self.__leafs.remove(parent)
        except ValueError:
            pass
        self.__leafs.append(node)
        self.__sequence_map[rev.sequence] = node

    def lookup(self, doc_id, rev):
        for node in self.__sequence_map:
            if node.data.docid == doc_id and node.data.revid == rev:
                return node.data
        return None

    def depth(self, sequence):
        try:
            return self.__sequence_map[sequence].depth
        except KeyError:
            return -1

    def child_by_rev(self, parent, rev):
        if not isinstance(parent, DocumentRevision):
            raise ValueError("parent must be a DocumentRevision")
        p = self.__sequence_map[parent.sequence]
        for child in p:
            if child.data.revid == rev:
                return child.data
        return None

    def by_sequence(self, sequence):
        try:
            return self.__sequence_map[sequence].data
        except KeyError:
            return None

    def has_conflicts(self):
        count = 0
        for node in self.__leafs:
            if not node.data.deleted:
                count += 1
            if count > 0:
                return True
        return False

    def roots(self):
        return copy.copy(self.__roots)

    def leafs(self):
        return copy.copy(self.__leafs)

    def leaf_revids(self):
        return map(lambda leaf: leaf.data.revid, self.__leafs)

    def current_rev(self):
        for node in self.__leafs:
            if node.data.current:
                return node.data
        raise ValueError('no current leaf in this tree')

    def path_for_node(self, rev):
        if not isinstance(rev, DocumentRevision):
            raise ValueError('expected a DocumentRevision')
        node = self.__sequence_map[rev.sequence]
        ret = []
        while node is not None:
            ret.insert(0, node)
            if node.data.parent > 0:
                node = self.__sequence_map[node.data.parent]
            else:
                node = None
        return ret

    def path(self, rev):
        return map(lambda r: r.revid, self.path_for_node(rev))
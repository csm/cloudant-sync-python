from .DocumentRevision import DocumentRevision


class DocumentRevisionTree(object):
    def __init__(self, node=None):
        self.__roots = {}
        self.__leafs = []
        self.__sequence_map = {}
        if node is not None:
            if not isinstance(node, DocumentRevision):
                raise ValueError('node must be a DocumentRevision')
            self.__add_root(node)

    def __add_root(self, root):
        if root.parent > 0:
            raise ValueError('The added root DocumentRevision must be a valid root revision')

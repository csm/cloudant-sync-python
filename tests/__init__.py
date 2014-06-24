import os
import unittest


def get_tests():
    startdir = os.path.dirname(__file__)
    return unittest.TestLoader().discover(startdir, pattern='*.py')
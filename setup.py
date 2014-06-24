from setuptools import setup


setup(name='cloudant-sync-python',
      version='0.0.1',
      description='Local NoSQL datastore with push/pull replication to CouchDB/Cloudant.',
      author='Casey Marshall',
      author_email='casey.s.marshall@gmail.com',
      packages=['cloudant', 'cloudant.sync', 'cloudant.sync.datastore'],
      requires=['requests', 'enum34'],
      test_suite='tests.get_tests')
import unittest
import time

from cassandra.ttypes import *

import lazyboy.connection as conn
from lazyboy.exceptions import ErrorCassandraClientNotFound
from test_columnfamily import MockClient


class TestClient(unittest.TestCase):
    def setUp(self):
        self.pool = 'testing'
        self.__client = conn.Client
        conn.Client = MockClient
        conn._CLIENTS = {}
        conn._SERVERS = {self.pool: ['localhost:1234']}

    def tearDown(self):
        conn.Client = self.__client

    def test_add_pool(self):
        servers = ['localhost:1234', 'localhost:5678']
        conn.add_pool(__name__, servers)
        self.assert_(conn._SERVERS[__name__] == servers)

    def test_get_pool(self):
        client = conn.get_pool(self.pool)
        self.assert_(type(client) is conn.Client)

        self.assertRaises(TypeError, conn.Client)

    def test_InvalidGetSliceNoTable(self):
        return # ErrorInvalidRequest doesn't seem to exist
        client = conn.get_pool(self.pool)
        key = "1"
        table = "users"
        column = "test"
        start = -1
        end = -1
        self.assertRaises(ErrorInvalidRequest, client.get_slice,
                          (table, key, column, start, end))

    def test_InvalidClient(self):
        self.assertRaises(ErrorCassandraClientNotFound, conn.get_pool,
                          ("votegfdgdfgdfgdfgs"))

    def test_InsertBatchSuperColumnFamily(self):
        client = conn.get_pool(self.pool)
        timestamp = time.time()
        vote_id = "12345"
        url = "http://google.com/"
        votes = []
        columns = [SuperColumn(vote_id, [Column(name = "456", value = "fake values", timestamp = timestamp)])]
        
        cfmap = {"votes": columns}

        row = BatchMutationSuper(key = vote_id, cfmap = cfmap)
        
        self.assert_(client.batch_insert_super_column("URI", row, 0) == None)
            
    def test_GetSliceSuperColumn(self):
        return
        client = conn.get_pool(self.pool)
        key = "12345"
        table = "URI"
        column = "votes"
        start = -1
        end = -1
        results = client.get_slice_super(table, key, column, start, end)

        self.assert_(results[0].name == "12345")


if __name__ == '__main__':
    unittest.main()

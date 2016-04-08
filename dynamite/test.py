import unittest

class TestConnection(unittest.TestCase):

    def test_connections(self):

        from dynamite import connection

        host = 'http://localhost:8000'
        conn = connection.Connection(endpoint_url=host)
        self.assertEqual(conn.meta.client._endpoint.host, host)
        self.assertEqual(conn.meta.client._client_config.region_name, 'us-east-1')

class TestTables(unittest.TestCase):

    def test_create_table(self):

        from dynamite import tables
        import random

        table_name = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890')
        random.shuffle(table_name)
        table_name = ''.join(table_name)[:16]
        table_name = 'test_table'

        class TestTable(tables.Table):

            name = table_name
            endpoint_url = 'http://localhost:8000'


        table = TestTable()
        created1, item1, key1 = table.items.create(hash='123', item={'jopa': '1'})
        created2, item2, key2 = table.items.create(hash='123', item={'jopa': '2'})
        created3, item3, key3 = table.items.create(item={'jopa': 'hahahaha'})

        self.assertTrue(created1)
        self.assertTrue(created2)
        self.assertTrue(created3)

        table.items.update(key1['id'])
        table.items.delete(key1['id'])

        table.items.create(item={'1': {'2': {'3': '4'}}})

        TestTable().delete()

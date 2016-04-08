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
        from boto3.dynamodb.conditions import Key, Attr
        import random

        table_name = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890')
        random.shuffle(table_name)
        table_name = ''.join(table_name)[:16]

        class TestTable(tables.Table):

            name = table_name
            endpoint_url = 'http://localhost:8000'


        table = TestTable()
        created1, item1 = table.items.create(hash_attr='123', item={'jopa': '1'})
        created2, item2 = table.items.create(hash_attr='123', item={'jopa': '2'})
        created3, item3 = table.items.create(item={'jopa': 'hahahaha'})

        self.assertTrue(created1)
        self.assertTrue(created2)
        self.assertTrue(created3)

        table.items.update(item1)
        table.items.delete(item1)

        created4, item4 = table.items.create(item={'1': {'2': {'3': '4'}}})
        hash_attr = table.items.get_hash_from_item(item4)

        self.assertEqual(table.items.get(item=item4), item4)

        item4.update({'5': {'6': '7'}})
        table.items.put(item=item4)
        self.assertEqual(
            table.items.get_hash_from_item(table.items.get(item=item4)),
            table.items.get_hash_from_item(item4),
        )
        self.assertEqual(hash_attr, table.items.get_hash_from_item(item4))
        table.items.put(hash_attr=item4['id'])

        created, new_item = table.items.create(hash_attr='345')
        self.assertEqual(new_item['id'], '345')

        # singleton test
        class TestTable2(tables.Table):

            name = table_name
            endpoint_url = 'http://localhost:8000'

        self.assertEqual(TestTable(), TestTable())
        self.assertNotEqual(TestTable(), TestTable2())

        self.assertEqual(TestTable2().items.get(new_item), TestTable().items.get(new_item))
        t = TestTable()
        self.assertEqual(t.table(), t())
        str(t)

        item = {'key1': {'key2': {'key3': 'data3'}}, 'key4': 'data4'}
        t.items.create(item=item)
        results = t.items.scan(FilterExpression=Attr('key1.key2.key3').exists())
        self.assertEqual(results[0]['key1'], item['key1'])
        t.items.query(KeyConditionExpression=Key(t.get_hash_name()).eq(results[0][t.get_hash_name()]))

        TestTable().delete()

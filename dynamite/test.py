import unittest

import dynamite.fields
from dynamite import connection
from dynamite import config

config.Config().DEFAULT_CONNECTION.endpoint_url = 'http://localhost:8000'

def get_random_string(length=16):
    import random
    result = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890')
    random.shuffle(result)
    result = ''.join(result)[:length]
    return result

class MyConnection(connection.Connection):
    endpoint_url = connection.ConnectionOption('http://localhost:8000')

class TestConnection(unittest.TestCase):

    def test_connections(self):

        from dynamite import connection

        # class TestConnection(connection.Connection):
        #     endpoint_url = connection.ConnectionOption('http://localhost:8000')
        conn = connection.Connection()
        self.assertEqual(conn.meta.client._endpoint.host, config.Config().DEFAULT_CONNECTION.endpoint_url)
        self.assertEqual(conn.meta.client._client_config.region_name, 'us-east-1')

    def test_option(self):

        from dynamite import connection

        option = connection.ConnectionOption('123')

        self.assertEqual(option(), '123')
        self.assertEqual(option.value, '123')

    def test_in_table(self):

        from dynamite import connection, tables

        class MyTable(tables.Table):
            name = get_random_string()

            class Connection(connection.Connection):

                endpoint_url = connection.ConnectionOption('http://localhost:8000')
                region_name = connection.ConnectionOption('eu-central-1')

        t = MyTable()

        self.assertEqual(t.connection.meta.client._endpoint.host, 'http://localhost:8000')
        self.assertEqual(t.connection.meta.client._client_config.region_name, 'eu-central-1')


        Default = connection.build_connection('DefaultConnection', region_name='eu-central-1', endpoint_url='http://localhost:8000')

        class MyTable(tables.Table):
            name = get_random_string()

            Connection = Default

        t = MyTable()

        self.assertEqual(t.connection.meta.client._endpoint.host, 'http://localhost:8000')
        self.assertEqual(t.connection.meta.client._client_config.region_name, 'eu-central-1')


class TestTables(unittest.TestCase):

    def test_create_table(self):

        from dynamite import tables, connection
        from boto3.dynamodb.conditions import Key, Attr

        table_name = get_random_string()

        class TestTable(tables.Table):

            name = table_name
            Connection = MyConnection


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
            Connection = MyConnection

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


    def test_table_with_schema(self):
        from dynamite import tables, schema
        class MyTable(tables.Table):
            name = get_random_string()

            class Connection(connection.Connection):

                endpoint_url = connection.ConnectionOption('http://localhost:8000')
                region_name = connection.ConnectionOption('eu-central-1')

        t = MyTable()

        class MyInlineSchema(schema.Schema):
            name = dynamite.fields.StrField()

        class MySchema(schema.Schema):
            inline_schema = dynamite.fields.SchemaField(MyInlineSchema)

        s = MySchema()
        s.inline_schema.name = 'cool name'

        item = s.to_db()
        created, item = t.items.create(item)
        t.items.get(item)

        b = MySchema()
        a = b.to_python(item)
        self.assertEqual(a, b)
        self.assertEqual(a.inline_schema.name, 'cool name')

        t.delete()

    def test_fabric(self):
        from dynamite import tables

        Default = connection.build_connection('DefaultConnection', region_name='eu-central-1', endpoint_url='http://localhost:8000')

        t1 = tables.build_table('NewTable', name=get_random_string(), Connection=Default)
        t2 = tables.build_table('NewTable2', name=get_random_string(), Connection=Default)
        t1().delete()
        t2().delete()


class TestSchema(unittest.TestCase):

    def test_schema(self):

        from dynamite import schema

        class InSchemaTwo(schema.Schema):
            name = dynamite.fields.StrField(default='in_schema_2')

        class InSchema(schema.Schema):
            name = dynamite.fields.UnicodeField(default=u'in_schema_1')
            in_schema = dynamite.fields.SchemaField(InSchemaTwo)

        class MySchema(schema.Schema):

            arg_str = dynamite.fields.StrField(default='default')
            arg_list = dynamite.fields.ListField()
            arg_dict = dynamite.fields.DictField()
            arg_unicode = dynamite.fields.UnicodeField(default=u'unicode')
            def_arg_dict = dynamite.fields.DictField(default={'2': 2})
            arg_b64 = dynamite.fields.Base64Field()
            arg_pickle = dynamite.fields.PickleField()
            in_schema = dynamite.fields.SchemaField(InSchema)

        my_schema = MySchema()

        self.assertEqual(my_schema.arg_str, 'default')

        self.assertFalse(isinstance(my_schema.arg_str, dynamite.fields.BaseField))

        my_schema.arg_str = '123'
        self.assertEqual(my_schema.arg_str, '123')

        def test_valid():
            my_schema.arg_str = 123

        self.assertRaises(dynamite.fields.SchemaValidationError, test_valid)
        my_schema.arg_dict = {'1': '2'}
        self.assertEqual(my_schema.arg_dict['1'], '2')
        my_schema.arg_list = [1, 2]
        self.assertEqual(my_schema.arg_list, [1, 2])
        self.assertEqual(my_schema.def_arg_dict['2'], 2)

        d = my_schema.to_db()
        two_schema = MySchema()
        two_schema.to_python(d)
        self.assertEqual(two_schema.to_db()['in_schema']['in_schema']['name'], 'in_schema_2')
        self.assertEqual(two_schema.to_db()['in_schema']['name'], 'in_schema_1')
        self.assertEqual(two_schema.to_db()['arg_str'], '123')
        empty_dict = {}
        two_schema.to_python(empty_dict)
        self.assertEqual(two_schema.to_db()['arg_str'], '123')
        two_schema.defaults_to_python = True
        two_schema.to_python(empty_dict)
        self.assertEqual(two_schema.to_db()['arg_str'], 'default')
        two_schema.in_schema.in_schema.name = '123'
        self.assertEqual(two_schema.to_db()['in_schema']['in_schema']['name'], '123')

        two_schema.defaults_to_python = False

        two_schema.to_python({'arg_str': u'123'})
        self.assertEqual(two_schema.to_db()['arg_str'], '123')

        two_schema.to_python({'arg_str': '123'})
        self.assertEqual(two_schema.to_db()['arg_str'], '123')

        two_schema.to_python({'arg_unicode': u'test'})
        self.assertEqual(two_schema.to_db()['arg_unicode'], u'test')

        two_schema.to_python({'arg_unicode': 'test'})
        self.assertEqual(two_schema.to_db()['arg_unicode'], u'test')

        two_schema.arg_b64 = 'jopa'
        two_schema.arg_pickle = 'jopa'

        d = two_schema.to_db()
        my_schema.to_python(d)
        self.assertEqual(my_schema.arg_b64, 'jopa')
        self.assertEqual(my_schema.arg_pickle, 'jopa')

        d = {'in_schema': []}

        self.assertRaises(ValueError, lambda: my_schema.to_python(d))

        s = MySchema(arg_str='jopka')
        self.assertEqual(s.arg_str, 'jopka')

class TestModels(unittest.TestCase):

    def test_models(self):

        from dynamite import models

        class MyModel(models.Model):
            name = dynamite.fields.StrField()

        record = MyModel(name='jopka')

        self.assertEqual(record.name, 'jopka')
        record.save()
        t = MyModel().table
        item = t.items.get(record.to_db())
        self.assertEqual(item['name'], record.name)

        record.name = 'new name'
        record.save()

        t = MyModel().table

        item = t.items.get(record.to_db())
        self.assertEqual(item['name'], record.name)

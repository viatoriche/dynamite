import unittest

import dynamite.fields
from dynamite.config import dynamite_options

dynamite_options['CONNECTION']['endpoint_url'] = 'http://localhost:8000'

def get_random_string(length=16):
    import random
    result = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890')
    random.shuffle(result)
    result = ''.join(result)[:length]
    return result

class TestConnection(unittest.TestCase):

    def test_connections(self):

        from dynamite import connection

        conn = connection.Connection()
        self.assertEqual(conn.meta.client._endpoint.host, dynamite_options['CONNECTION']['endpoint_url'])
        self.assertEqual(conn.meta.client._client_config.region_name, 'us-east-1')

    def test_in_table(self):

        from dynamite import connection, tables

        dynamite_options['CONNECTION']['region_name'] = 'eu-central-1'
        connection.Connection().rebuild()

        t = tables.Table(get_random_string())

        self.assertEqual(t.connection.meta.client._endpoint.host, 'http://localhost:8000')
        self.assertEqual(t.connection.meta.client._client_config.region_name, 'eu-central-1')


        t = tables.Table(get_random_string())

        self.assertEqual(t.connection.meta.client._endpoint.host, 'http://localhost:8000')
        self.assertEqual(t.connection.meta.client._client_config.region_name, 'eu-central-1')


class TestTables(unittest.TestCase):

    def test_create_table(self):

        from dynamite import tables
        from boto3.dynamodb.conditions import Key, Attr

        table_name = get_random_string()

        table = tables.Table(table_name)
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

        item = {'key1': {'key2': {'key3': 'data3'}}, 'key4': 'data4'}
        table.items.create(item=item)
        results = table.items.scan(FilterExpression=Attr('key1.key2.key3').exists())
        self.assertEqual(results[0]['key1'], item['key1'])
        table.items.query(KeyConditionExpression=Key(table.get_hash_name()).eq(results[0][table.get_hash_name()]))

        table.delete()


    def test_table_with_schema(self):
        from dynamite import tables, schema
        table_name = get_random_string()

        t = tables.Table(table_name)

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

        class CustomHashRanges(schema.Schema):

            custom_id = dynamite.fields.StrField(hash_field=True)
            custom_range = dynamite.fields.StrField(range_field=True)

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

        c = CustomHashRanges()
        self.assertEqual(c._hash_field, 'custom_id')
        self.assertEqual(c._range_field, 'custom_range')

class TestModels(unittest.TestCase):

    def test_models(self):

        from dynamite import models, tables, fields

        add_name = get_random_string()

        # @models.generate_table
        class MyModel(models.Model):
            name = dynamite.fields.StrField()

            @classmethod
            def get_table_name(cls):
                return '{}Table_{}'.format(cls.__name__, add_name)

        self.assertEqual(MyModel.hash, 'id')
        self.assertEqual(MyModel.range, None)
        self.assertEqual(MyModel.table, MyModel().table)
        record = MyModel(name='jopka')


        self.assertEqual(record.name, 'jopka')
        record.save()
        t = MyModel.get_table()
        item = t.items.get(record.to_db())
        self.assertNotEqual(item, None)
        self.assertEqual(item['name'], record.name)
        self.assertEqual(record.id, record.hk)

        record.name = 'new name'
        record.save()

        t = MyModel.get_table()

        item = t.items.get(record.to_db())
        self.assertEqual(item['name'], record.name)

        t.delete()


        class ModelIDRange(models.Model):

            custom_id = dynamite.fields.StrField(hash_field=True, default='my_super_hash')
            custom_range = dynamite.fields.StrField(range_field=True)
            name = dynamite.fields.StrField(default=lambda: 'name')
            empty = dynamite.fields.StrField()

            @classmethod
            def hash_generator(cls):
                return 'my_super_hash'

            @classmethod
            def get_table_name(cls):
                return '{}Table_{}'.format(cls.__name__, add_name)

        m = ModelIDRange(custom_range='CUSTOM_RANGE')
        m.save()
        t = m.get_table()
        self.assertEqual(m.custom_range, 'CUSTOM_RANGE')
        self.assertEqual(m.custom_id, 'my_super_hash')
        self.assertEqual(m.name, 'name')
        self.assertEqual(m.empty, '')
        self.assertEqual(m.hk, m.custom_id)
        self.assertEqual(m.rk, m.custom_range)
        self.assertEqual(ModelIDRange.hash, 'custom_id')
        self.assertEqual(ModelIDRange.range, 'custom_range')
        self.assertRaises(RuntimeError, lambda: ModelIDRange(custom_range='CUSTOM_RANGE').save())

        class NewModel(models.Model):

            @classmethod
            def get_table_name(cls):
                return 'test_non_create'

        class NewModel2(models.Model):

            @classmethod
            def get_table_name(cls):
                return 'test_non_create'

        NewModel.get_table().scan()
        NewModel2.get_table().scan()

        t.delete()

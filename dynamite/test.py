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


class TestItems(unittest.TestCase):

    def test_items(self):

        from dynamite import items
        class FakeTable(object):

            def get_range_name(self):
                return 'range'

            def get_hash_name(self):
                return 'hash'

        table_items = items.TableItems(table=FakeTable())
        self.assertEqual(table_items.generate_key(range_attr='r123', hash_attr='h123')['range'], 'r123')
        self.assertEqual(table_items.generate_key(range_attr='r123', hash_attr='h123')['hash'], 'h123')

class TestTables(unittest.TestCase):

    def test_create_table(self):

        from dynamite import tables, items
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
        results = list(table.items.scan(FilterExpression=Attr('key1.key2.key3').exists()))
        self.assertEqual(results[0]['key1'], item['key1'])
        results = list(table.items.query(KeyConditionExpression=Key(table.get_hash_name()).eq(results[0][table.get_hash_name()])))
        self.assertEqual(results[0]['key4'], 'data4')
        results = list(table.items.all())
        self.assertEqual(len(results), 5)

        class MyItems(items.TableItems):
            for_test = 'Jopka'

        table1 = tables.Table(table_name, items=MyItems)
        table2 = tables.Table(table_name, items=MyItems())

        self.assertEqual(table1.items.for_test, table2.items.for_test)
        self.assertEqual(table1.items.for_test, 'Jopka')

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

        self.assertEqual(str(t), "<dynamodb.Table(name=u'{}')>".format(t.name))
        self.assertEqual(t(), t.table)
        self.assertEqual(t.get_map_attr('1', '2', '3'), '1.2.3')
        t.delete()

class TestSchema(unittest.TestCase):

    def test_schema(self):

        from dynamite import schema, fields

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
            arg_number = dynamite.fields.DynamoNumberField()
            arg_string = dynamite.fields.DynamoStringField()
            arg_bool = dynamite.fields.BooleanField()

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
        my_schema.arg_string = '123'
        self.assertEqual(my_schema.arg_string, u'123')
        self.assertEqual(my_schema.arg_string, '123')
        my_schema.arg_string = u'123'
        self.assertEqual(my_schema.arg_string, u'123')
        self.assertEqual(my_schema.arg_string, '123')
        my_schema.arg_number = 123
        self.assertEqual(my_schema.arg_number, 123)
        def add_number():
            my_schema.arg_number = 'no number'
        self.assertRaises(fields.SchemaValidationError, add_number)

        d = my_schema.to_db()
        two_schema = MySchema()
        two_schema.to_python(d)
        self.assertEqual(two_schema.to_db()['in_schema']['in_schema']['name'], 'in_schema_2')
        self.assertEqual(two_schema.to_db()['in_schema']['name'], 'in_schema_1')
        self.assertEqual(two_schema.to_db()['arg_str'], '123')
        empty_dict = {}
        two_schema.to_python(empty_dict)
        self.assertEqual(two_schema.to_db()['arg_str'], '123')
        two_schema._defaults_to_python = True
        two_schema.to_python(empty_dict)
        self.assertEqual(two_schema.to_db()['arg_str'], 'default')
        two_schema.in_schema.in_schema.name = '123'
        self.assertEqual(two_schema.to_db()['in_schema']['in_schema']['name'], '123')

        two_schema._defaults_to_python = False

        two_schema.to_python({'arg_str': u'123'})
        self.assertEqual(two_schema.to_db()['arg_str'], '123')

        two_schema.to_python({'arg_str': '123'})
        self.assertEqual(two_schema.to_db()['arg_str'], '123')

        two_schema.to_python({'arg_unicode': u'test'})
        self.assertEqual(two_schema.to_db()['arg_unicode'], u'test')

        two_schema.to_python({'arg_unicode': 'test'})
        self.assertEqual(two_schema.to_db()['arg_unicode'], u'test')

        two_schema.to_python({'in_schema': {'in_schema': {'name': 'new name'}}})
        self.assertEqual(two_schema.to_db()['in_schema']['in_schema']['name'], 'new name')

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

        s['arg_str'] = 'popka'
        self.assertEqual(s['arg_str'], 'popka')
        self.assertEqual(s.arg_str, 'popka')

        self.assertEqual(s['none'], None)

        self.assertEqual(str(InSchemaTwo()), '<InSchemaTwo: name=StrField>')

class TestModels(unittest.TestCase):

    def test_models(self):

        from dynamite import models, tables, defines, fields, items

        add_name = get_random_string()

        class NameModel(models.Model):
            pass

        self.assertEqual(NameModel.get_table_name(), 'NameModel')

        class NumberTable(tables.Table):

            def __init__(self, *args, **kwargs):
                super(NumberTable, self).__init__(*args, **kwargs)
                self.hash_attr = ('number_id', defines.NUMBER)
                self.range_attr = ('range_str', defines.STRING)

            def hash_generator(table):
                import random
                return random.randint(0, 1024)

        class NumberRangeTable(tables.Table):

            def __init__(self, *args, **kwargs):
                super(NumberRangeTable, self).__init__(*args, **kwargs)
                self.range_attr = ('range_int', defines.NUMBER)

        class NumberID(models.Model):

            Table = NumberTable

            @classmethod
            def get_table_name(cls):
                return '{}Table_{}'.format(cls.__name__, add_name)

        class NumberRange(models.Model):

            Table = NumberRangeTable

            @classmethod
            def get_table_name(cls):
                return '{}Table_{}'.format(cls.__name__, add_name)

        m = NumberID(range_str=u'123')
        m.save()
        self.assertTrue(isinstance(m.number_id, int))
        self.assertEqual(m.range_str, '123')
        self.assertTrue(isinstance(m.__class__.number_id, fields.IntField))
        m.table.delete()

        m = NumberRange(range_int=123)
        m.save()
        self.assertTrue(isinstance(m.range_int, int))
        self.assertEqual(m.range_int, 123)
        self.assertTrue(isinstance(m.__class__.range_int, fields.IntField))
        m.table.delete()

        class MyModel(models.Model):
            name = dynamite.fields.StrField(name='another_name')

            @classmethod
            def get_table_name(cls):
                return '{}Table_{}'.format(cls.__name__, add_name)

        self.assertEqual(MyModel.hash, 'id')
        self.assertEqual(MyModel.range, None)
        self.assertEqual(MyModel.table, MyModel().table)
        record = MyModel(another_name='jopka')
        self.assertEqual(record.rk, None)
        self.assertEqual(MyModel.items, MyModel.get_items())
        self.assertEqual(MyModel.items, record.items)

        class PickleModel(models.Model):
            pickle = fields.PickleField()

        pm = PickleModel()
        pm.pickle = {'test': 123}
        self.assertEqual(pm.pickle, {'test': 123})
        pm.save()
        self.assertEqual(pm.pickle, {'test': 123})

        pm2 = PickleModel.get(id=pm.id)
        self.assertEqual(pm2.pickle, {'test': 123})

        record = MyModel(another_name='popka')
        self.assertEqual(record.another_name, 'popka')
        record.save()
        self.assertEqual('<MyModel: {}>'.format(record._key), str(record))
        self.assertEqual(record.rk, None)
        t = MyModel.get_table()
        item = t.items.get(record.to_db())
        self.assertNotEqual(item, None)
        self.assertEqual(item['another_name'], record.another_name)
        self.assertEqual(record.id, record.hk)

        record.name = 'new name'
        record.save()

        t = MyModel.get_table()

        item = t.items.get(record.to_db())
        self.assertEqual(item['another_name'], record.another_name)

        t.delete()


        class ModelIDRange(models.Model):

            custom_id = dynamite.fields.StrField(hash_field=True, default='my_super_hash')
            custom_range = dynamite.fields.StrField(range_field=True)
            name = dynamite.fields.StrField(default=lambda: 'name')
            empty = dynamite.fields.StrField()
            pickle = dynamite.fields.PickleField()

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
        m2 = ModelIDRange(custom_range='CUSTOM_RANGE')
        m2.pickle = {'test': 123}
        m2.save()
        mm = ModelIDRange.get(custom_id=m2.custom_id, custom_range=m2.custom_range)
        self.assertEqual(mm.pickle, {'test': 123})
        m = ModelIDRange.get(custom_id='my_super_hash', custom_range='CUSTOM_RANGE')
        self.assertFalse(m is None)

        class NewModel(models.Model):

            @classmethod
            def get_table_name(cls):
                return 'test_non_create'

        class CustomItems(items.TableItems):
            for_test = 'Popka'

        class NewModel2(models.Model):

            Items = CustomItems

            @classmethod
            def get_table_name(cls):
                return 'test_non_create'

        self.assertEqual(list(NewModel.get_table().items.scan()), list(NewModel2.get_table().items.scan()))
        self.assertEqual(NewModel2.items.for_test, 'Popka')

        t.delete()

        class ModelTestRecurse(models.Model):

            @classmethod
            def hash_generator(cls):
                return 'my_super_hash'

            @classmethod
            def get_table_name(cls):
                return '{}Table_{}'.format(cls.__name__, add_name)

        m = ModelTestRecurse()
        m.save()
        m = ModelTestRecurse()
        self.assertRaises(RuntimeError, lambda: m.save())
        m.table.delete()

        test_name = get_random_string()

        class ModelTestItems(models.Model):
            test = fields.Base64Field()
            num = fields.DynamoNumberField()

            @classmethod
            def get_table_name(cls):
                return test_name

        t1 = ModelTestItems()
        t2 = ModelTestItems()
        t3 = ModelTestItems()
        t3.save()
        t1.test = '\x00'
        t1.num = 1
        t2.test = '\x01'
        t2.num = 2
        t3.test = '\x02'
        t3.num = 3
        t1.save()
        t2.save()
        t3.save()

        self.assertEqual(t1.test, '\x00')
        self.assertEqual(t2.test, '\x01')
        self.assertEqual(t3.test, '\x02')

        t1 = ModelTestItems.get(id=t1.id)
        self.assertEqual(t1.test, '\x00')

        from boto3.dynamodb.conditions import Attr

        results = list(ModelTestItems.scan(FilterExpression=Attr('num').eq(1)))

        self.assertEqual(results[0].num, 1)
        self.assertEqual(results[0].test,'\x00')
        self.assertEqual(len(results), 1)

        results = [m.num for m in ModelTestItems.all()]

        self.assertTrue(1 in results)
        self.assertTrue(2 in results)
        self.assertTrue(3 in results)

        results = [m.test for m in ModelTestItems.all()]

        self.assertTrue('\x00' in results)
        self.assertTrue('\x01' in results)
        self.assertTrue('\x02' in results)

        self.assertEqual(len(results), 3)

        ModelTestItems.delete(id=t1.id)
        t1 = ModelTestItems.get(id=t1.id)
        self.assertEqual(t1, None)



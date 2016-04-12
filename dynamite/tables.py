import uuid

from botocore import exceptions as boto_exceptions
from dynamite import connection
from dynamite import defines
from dynamite.patterns import Singleton


class KeyValidationError(ValueError):
    pass


class TableItems(object):


    def __init__(self, table=None, max_recursion_create=5):
        self.table = table
        self.max_recursion_create = max_recursion_create

    def generate_key(self, item=None, hash_attr=None, range_attr=None):
        key = {}
        if item is not None:
            key.update(self.get_key_from_item(item))
        if range_attr is not None:
            range_name = self.table.get_range_name()
            key.update(
                {
                    range_name: range_attr,
                }
            )
        if hash_attr is not None:
            hash_name = self.table.get_hash_name()
            key.update(
                {
                    hash_name: hash_attr,
                }
            )

        return key

    def get_hash_from_item(self, item):
        return item.get(self.table.get_hash_name(), None)

    def get_range_from_item(self, item):
        return item.get(self.table.get_range_name(), None)

    def get_key_from_item(self, item):
        key = {self.table.get_hash_name(): self.get_hash_from_item(item)}
        range_value = self.get_range_from_item(item)
        if range_value is not None:
            key[self.table.get_range_name()] = range_value
        return key

    def update(self, item=None, hash_attr=None, range_attr=None, **options):
        key = self.generate_key(item=item, hash_attr=hash_attr, range_attr=range_attr)

        options.update({'Key': key})
        response = self.table.update_item(**options)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result

    def delete(self, item=None, hash_attr=None, range_attr=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, range_attr=range_attr)

        response = self.table.delete_item(Key=key)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result

    def put(self, item=None, hash_attr=None, range_attr=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, range_attr=range_attr)
        if item is None:
            item = {}
        item.update(key)
        response = self.table.put_item(Item=item)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result, item

    def get(self, item=None, hash_attr=None, range_attr=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, range_attr=range_attr)
        response = self.table.get_item(
            Key=key,
        )
        return response.get('Item', None)

    def create(self, item=None, hash_attr=None, range_attr=None, _recurse_count=0):
        if _recurse_count > self.max_recursion_create:
            raise RuntimeError('Maximum tries for create...')
        key = self.generate_key(item=item, hash_attr=hash_attr, range_attr=range_attr)
        if item is None:
            item = {}

        item.update(key)
        hash_attr = self.get_hash_from_item(item)
        if not hash_attr:
            hash_attr = self.table.hash_generator(self.table)
            key.update(self.generate_key(hash_attr=hash_attr))

        item.update(key)

        if self.get(item=item) is None:
            return self.put(item=item)
        else:
            hash_attr = self.table.hash_generator(self.table)
            item.update(self.generate_key(hash_attr=hash_attr))
            return self.create(item=item, _recurse_count=_recurse_count+1)

    def scan(self, **options):
        response = self.table.scan(**options)
        items = response.get('Items', [])
        return items

    def query(self, **options):
        response = self.table.query(**options)
        items = response.get('Items', [])
        return items

    def all(self):
        response = self.table.scan()
        items = response.get('Items', [])
        return items


class Table(Singleton):
    name = None
    _connection = None
    hash_attr = ('id', defines.STRING,)
    range_attr = []
    read_capacity_units = 5
    write_capacity_units = 5
    _table = None

    items = TableItems()

    Connection = connection.Connection

    @staticmethod
    def hash_generator(table):
        return str(uuid.uuid4())

    def __str__(self):
        return self.table().__str__()

    def __init__(self):
        self.items.table = self

    def __call__(self, *args, **kwargs):
        """
        get boto3 table
        """
        return self.table()

    def get_map_attr(self, *args):
        return '.'.join([str(arg) for arg in args if arg])

    @classmethod
    def get_connection(cls):
        if cls._connection is None:
            cls._connection = cls.Connection()
        return cls._connection

    @property
    def connection(self):
        return self.get_connection()

    @classmethod
    def get_hash_schema(cls):
        return {
            'AttributeName': cls.get_hash_name(),
            'KeyType': defines.HASH,
        }

    @classmethod
    def get_range_schema(cls):
        if cls.range_attr:
            return {
                'AttributeName': cls.get_range_name(),
                'KeyType': defines.RANGE,
            }
        else:
            return {}

    @classmethod
    def get_key_schema(cls):

        return [value for value in [cls.get_hash_schema(), cls.get_range_schema()] if value]

    @classmethod
    def get_range_attribute_list(cls):
        if cls.range_attr:
            return [
                {
                    'AttributeName': cls.get_range_name(),
                    'AttributeType': cls.get_range_type(),
                }
            ]
        return []

    @classmethod
    def get_hash_attribute_list(cls):
        return [
             {
                 'AttributeName': cls.get_hash_name(),
                 'AttributeType': cls.get_hash_type(),
             }
        ]

    @classmethod
    def get_hash_name(cls):
        return cls.hash_attr[0]

    @classmethod
    def get_hash_type(cls):
        return cls.hash_attr[1]

    @classmethod
    def get_range_name(cls):
        if cls.range_attr:
            return cls.range_attr[0]

    @classmethod
    def get_range_type(cls):
        if cls.range_attr:
            return cls.range_attr[1]

    @classmethod
    def get_attribute_definitions(cls):
        return cls.get_hash_attribute_list() + cls.get_range_attribute_list()

    @classmethod
    def create(cls):
        table = cls.get_connection().create_table(
            TableName=cls.name,
            KeySchema=cls.get_key_schema(),
            AttributeDefinitions=cls.get_attribute_definitions(),
            ProvisionedThroughput={
                'ReadCapacityUnits': cls.read_capacity_units,
                'WriteCapacityUnits': cls.write_capacity_units,
            },
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=cls.name)
        return table

    @classmethod
    def update(cls):
        result = cls.table().update(
            AttributeDefinitions=cls.get_attribute_definitions(),
            ProvisionedThroughput={
                'ReadCapacityUnits': cls.read_capacity_units,
                'WriteCapacityUnits': cls.write_capacity_units,
            },
        )
        return result

    @classmethod
    def get(cls):
        table = cls.get_connection().Table(cls.name)
        return table

    @classmethod
    def table(cls):
        if cls._table is None:
            try:
                cls._table = cls.create()
            except boto_exceptions.ClientError as e:
                if e.response['Error']['Code'] == u'ResourceInUseException':
                    cls._table = cls.get()
                else:
                    raise e
        return cls._table

    def __getattr__(self, item):
        """
        routing attr to boto3 table
        :param item:
        :return:
        """
        table = self.table()
        return getattr(table, item)


def build_table(classname, **kwargs):
    kwargs['name'] = kwargs.pop('name', classname)
    return type(classname, (Table,), kwargs)

import uuid

from botocore import exceptions as boto_exceptions
from dynamite import connection
from dynamite import defines

class Singleton(object):
    """Singleton class"""

    __instances = {}

    def __new__(cls, *args, **kwargs):
        instance = Singleton.__instances.get(cls)
        if instance is None:
            Singleton.__instances[cls] = object.__new__(cls, *args, **kwargs)
            instance = Singleton.__instances[cls]
        return instance

class KeyValidationError(ValueError):
    pass


class TableItems(object):
    def generate_hash(self):
        return str(uuid.uuid4())

    def __init__(self, table=None):
        self.table = table

    def generate_key(self, item=None, hash_attr=None, ranges=None):
        key = {}
        if item is not None:
            key.update(self.get_key_from_item(item))
        if ranges is None:
            ranges = {}
        if hash_attr is not None:
            hash_name = self.table.get_hash_name()
            key.update(
                {
                    hash_name: hash_attr,
                }
            )
        key.update({rng: ranges[rng] for rng in ranges if ranges[rng] is not None})
        return key

    def get_hash_from_item(self, item):
        return item.get(self.table.get_hash_name(), None)

    def get_ranges_from_item(self, item):
        return {rng: item.get(rng, None) for rng in self.table.ranges if rng in item}

    def get_key_from_item(self, item):
        hash_key = {self.table.get_hash_name(): self.get_hash_from_item(item)}
        ranges = self.get_ranges_from_item(item)
        ranges.update(hash_key)
        key = {k: ranges[k] for k in ranges if ranges[k] is not None}
        return key

    def update(self, item=None, hash_attr=None, ranges=None, **options):
        key = self.generate_key(item=item, hash_attr=hash_attr, ranges=ranges)

        options.update({'Key': key})
        response = self.table.update_item(**options)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result

    def delete(self, item=None, hash_attr=None, ranges=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, ranges=ranges)

        response = self.table.delete_item(Key=key)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result

    def put(self, item=None, hash_attr=None, ranges=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, ranges=ranges)
        if item is None:
            item = {}
        item.update(key)
        response = self.table.put_item(Item=item)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result, item

    def get(self, item=None, hash_attr=None, ranges=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, ranges=ranges)
        response = self.table.get_item(
            Key=key,
        )
        return response.get('Item', None)

    def create(self, item=None, hash_attr=None, ranges=None):
        key = self.generate_key(item=item, hash_attr=hash_attr, ranges=ranges)
        if item is None:
            item = {}

        item.update(key)
        hash_attr = self.get_hash_from_item(item)
        if hash_attr is None:
            hash_attr = self.generate_hash()
            key.update(self.generate_key(hash_attr=hash_attr))

        if ranges is None:
            ranges = self.get_ranges_from_item(item)
            key.update(self.generate_key(ranges=ranges))

        item.update(key)

        if self.get(item=item) is None:
            return self.put(item=item)
        else:
            hash_attr = self.generate_hash()
            item.update(self.generate_key(hash_attr=hash_attr))
            return self.create(item=item)

    def scan(self, **options):
        response = self.table.scan(**options)
        items = response.get('Items', [])
        return items

    def query(self, **options):
        response = self.table.query(**options)
        items = response.get('Items', [])
        return items


class Table(Singleton):
    name = None
    _connection = None
    conn_options = None
    hash = ('id', defines.STRING,)
    ranges = []
    hash_type = defines.STRING
    range_types = []
    read_capacity_units = 5
    write_capacity_units = 5
    _table = None

    items = TableItems()

    def __str__(self):
        return self.table().__str__()

    def __init__(self):
        self.items.table = self

    def __call__(self, *args, **kwargs):
        """
        get boto3 table
        """
        return self.table()

    @classmethod
    def connection(cls):
        if cls._connection is None:
            if cls.conn_options is None:
                cls.conn_options = {}
            cls._connection = connection.Connection(**cls.conn_options)
        return cls._connection

    @classmethod
    def get_key_schema(cls):

        key_schema = [
            {
                'AttributeName': cls.get_hash_name(),
                'KeyType': defines.HASH,
            },
        ]

        ranges = [
            {
                'AttributeName': attr[0],
                'KeyType': defines.RANGE,
            } for attr in cls.ranges
            ]
        key_schema += ranges

        return key_schema

    @classmethod
    def get_hash_name(cls):
        return cls.hash[0]

    @classmethod
    def get_hash_type(cls):
        return cls.hash[1]

    @classmethod
    def get_attribute_definitions(cls):

        attribute_definitions = [
                                    {
                                        'AttributeName': cls.get_hash_name(),
                                        'AttributeType': cls.get_hash_type(),
                                    }
                                ] + [
                                    {
                                        'AttributeName': attr[0],
                                        'AttributeType': attr[1],
                                    } for attr in cls.ranges
                                    ]
        return attribute_definitions

    @classmethod
    def create(cls):
        connection = cls.connection()

        table = connection.create_table(
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
    def get(cls):
        connection = cls.connection()
        table = connection.Table(cls.name)
        return table

    @classmethod
    def table(cls):
        if cls._table is None:
            try:
                cls._table = cls.create()
            except boto_exceptions.ClientError:
                cls._table = cls.get()
        return cls._table

    def __getattr__(self, item):
        """
        routing attr to boto3 table
        :param item:
        :return:
        """
        table = self.table()
        return getattr(table, item)

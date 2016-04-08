from dynamite import connection
from dynamite import defines
from botocore import exceptions as boto_exceptions
import uuid

class KeyValidationError(ValueError):
    pass

class TableItems(object):

    def generate_hash(self):
        return str(uuid.uuid4())

    def __init__(self, table=None):
        self.table = table

    def validate_key(self, key):
        hash_name = self.table.get_hash_name()
        if hash_name not in key:
            raise KeyValidationError('hash {} not in key'.format(hash_name))
        # for rng in self.table.ranges:
        #     range_name = rng[0]
        #     if range_name not in key:
        #         raise KeyValidationError('range {} not in key'.format(range_name))

    def generate_key(self, hash, ranges=None):
        if ranges is None:
            ranges = {}
        hash_name = self.table.get_hash_name()
        key = {
            hash_name: hash,
        }
        for rng in ranges:
            key[rng] = ranges[rng]
        self.validate_key(key)
        return key

    def get_hash_from_item(self, item):
        return item[self.table.get_hash_name()]

    def get_ranges_from_item(self, item):
        return {rng: item[rng] for rng in self.table.ranges}

    def get_key_from_item(self, item):
        key = {self.table.get_hash_name(): self.get_hash_from_item(item)}
        ranges = self.get_ranges_from_item(item)
        ranges.update(key)
        return ranges

    def update(self, hash, ranges=None, **options):
        key = self.generate_key(hash, ranges)
        options.update({'Key': key})
        response = self.table.update_item(**options)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result

    def delete(self, hash, ranges=None):
        response = self.table.delete_item(Key=self.generate_key(hash, ranges))
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result

    def put(self, hash, ranges=None, item=None):
        if item is None:
            item = {}
        key = self.generate_key(hash, ranges)
        item.update(key)
        response = self.table.put_item(Item=item)
        result = response['ResponseMetadata']['HTTPStatusCode'] == 200
        return result, item, self.get_key_from_item(item)

    def get(self, hash, ranges=None):
        key = self.generate_key(hash, ranges)
        response = self.table.get_item(
            Key=key,
        )
        return response.get('Item', None)

    def hash_name(self):
        return self.table.get_hash_name()

    def create(self, hash=None, ranges=None, item=None):
        if hash is None:
            hash = self.generate_hash()

        if self.get(hash, ranges) is None:
            return self.put(hash, ranges, item)
        else:
            hash = self.generate_hash()
            return self.create(hash=hash, ranges=ranges, item=item)

    def scan(self, **options):
        response = self.table.scan(**options)
        items = response.get('Items', [])
        return items

    def query(self, **options):
        response = self.table.query(**options)
        items = response.get('Items', [])
        return items


class Table(object):
    name = None
    _connection = None
    conn_options = None
    hash = ('id', defines.STRING, )
    ranges = []
    hash_type = defines.STRING
    range_types = []
    read_capacity_units = 5
    write_capacity_units = 5

    items = TableItems()

    def __str__(self):
        return self.table().__str__()

    def __init__(self):
        self.items.table = self

    def __call__(self, *args, **kwargs):
        return self

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
        try:
            table = cls.create()
        except boto_exceptions.ClientError:
            table = cls.get()
        return table

    def __getattr__(self, item):
        table = self.table()
        return getattr(table, item)

import uuid

from botocore import exceptions as boto_exceptions
from dynamite import connection
from dynamite import defines


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
            hash_attr = self.table.hash_generator()
            key.update(self.generate_key(hash_attr=hash_attr))

        item.update(key)

        if self.get(item=item) is None:
            return self.put(item=item)
        else:
            hash_attr = self.table.hash_generator()
            item.update(self.generate_key(hash_attr=hash_attr))
            return self.create(item=item, _recurse_count=_recurse_count + 1)

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


class Table(object):
    def hash_generator(table):
        return str(uuid.uuid4())

    def __str__(self):
        return self.table.__str__()

    def __init__(self, name, hash_attr=None, range_attr=None, items=None, read_capacity_units=5,
                 write_capacity_units=5, hash_generator=None):
        self.name = name
        if hash_attr is None:
            hash_attr = ('id', defines.STRING,)
        self.hash_attr = hash_attr
        if range_attr is None:
            range_attr = []
        self.range_attr = range_attr

        self.read_capacity_units = read_capacity_units
        self.write_capacity_units = write_capacity_units
        self._table = None
        if items is None:
            items = TableItems(self)

        self.items = items
        self.items.table = self
        if hash_generator is not None:
            self.hash_generator = hash_generator

    def __call__(self, *args, **kwargs):
        """
        get boto3 table
        """
        return self.table

    def get_map_attr(self, *args):
        return '.'.join([str(arg) for arg in args if arg])

    @property
    def connection(self):
        return connection.Connection()

    def get_hash_schema(self):
        return {
            'AttributeName': self.get_hash_name(),
            'KeyType': defines.HASH,
        }

    def get_range_schema(self):
        if self.range_attr:
            return {
                'AttributeName': self.get_range_name(),
                'KeyType': defines.RANGE,
            }
        else:
            return {}

    def get_key_schema(self):
        return [value for value in [self.get_hash_schema(), self.get_range_schema()] if value]

    def get_range_attribute_list(self):
        if self.range_attr:
            return [
                {
                    'AttributeName': self.get_range_name(),
                    'AttributeType': self.get_range_type(),
                }
            ]
        return []

    def get_hash_attribute_list(self):
        return [
            {
                'AttributeName': self.get_hash_name(),
                'AttributeType': self.get_hash_type(),
            }
        ]

    def get_hash_name(self):
        return self.hash_attr[0]

    def get_hash_type(self):
        return self.hash_attr[1]

    def get_range_name(self):
        if self.range_attr:
            return self.range_attr[0]

    def get_range_type(self):
        if self.range_attr:
            return self.range_attr[1]

    def get_attribute_definitions(self):
        return self.get_hash_attribute_list() + self.get_range_attribute_list()

    def _create(self):
        table = self.connection.create_table(
            TableName=self.name,
            KeySchema=self.get_key_schema(),
            AttributeDefinitions=self.get_attribute_definitions(),
            ProvisionedThroughput={
                'ReadCapacityUnits': self.read_capacity_units,
                'WriteCapacityUnits': self.write_capacity_units,
            },
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=self.name)
        return table

    def update(self):
        result = self.table.update(
            AttributeDefinitions=self.get_attribute_definitions(),
            ProvisionedThroughput={
                'ReadCapacityUnits': self.read_capacity_units,
                'WriteCapacityUnits': self.write_capacity_units,
            },
        )
        return result

    def _get_table(self):
        table = self.connection.Table(self.name)
        return table

    @property
    def table(self):
        if self._table is None:
            try:
                self._table = self._create()
            except boto_exceptions.ClientError as e:
                if e.response['Error']['Code'] == u'ResourceInUseException':
                    self._table = self._get_table()
                else:
                    raise e
        return self._table

    def __getattr__(self, item):
        """
        routing attr to boto3 table
        :param item:
        :return:
        """
        return getattr(self.table, item)

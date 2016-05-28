import uuid

from botocore import exceptions as boto_exceptions
from dynamite import connection
from dynamite import defines
from dynamite.items import TableItems
import inspect


class KeyValidationError(ValueError):
    pass


class Table(object):
    def hash_generator(table):
        return str(uuid.uuid4())

    def __str__(self):
        return '<{}>'.format(self.table.__str__())

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
        else:
            if inspect.isclass(items):
                items = items()

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

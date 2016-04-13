from dynamite import fields
from dynamite.defines import STRING, NUMBER
from dynamite.schema import Schema
from dynamite.tables import Table
from dynamite.utils import ClassProperty


class Model(Schema):
    _table = None
    read_capacity_units = 5
    write_capacity_units = 5

    # ignore class properties in get_fields
    _ignore_elems = set(['table', 'items', 'hash', 'range'])

    # @classmethod
    hash_generator = None

    def __init__(self, **kwargs):
        self.key = kwargs.pop('key', {})
        super(Model, self).__init__(**kwargs)
        if self.__class__._table is None:
            self.generate_table()

    @classmethod
    def generate_table(cls):
        cls.get_fields()
        range_attr = None
        if cls._range_field is not None:
            range_attr = [cls._range_field, cls._fields[cls._range_field].db_type]
        hash_attr = None
        if cls._hash_field is not None:
            hash_attr = [cls._hash_field, cls._fields[cls._hash_field].db_type]
        cls._table = Table(
            name=cls.get_table_name(),
            hash_attr=hash_attr,
            range_attr=range_attr,
            read_capacity_units=cls.read_capacity_units,
            write_capacity_units=cls.write_capacity_units,
            hash_generator=cls.hash_generator,
        )
        if cls._hash_field is None:
            field = cls._table.hash_attr[0]
            if cls._table.hash_attr[1] == STRING:
                setattr(cls, field, fields.UnicodeField(hash_field=True))
            if cls._table.hash_attr[1] == NUMBER:
                setattr(cls, field, fields.IntField(hash_field=True))
        if cls._range_field is None:
            if cls._table.range_attr:
                field = cls._table.range_attr[0]
                if cls._table.range_attr[1] == STRING:
                    setattr(cls, field, fields.UnicodeField(range_field=True))
                if cls._table.range_attr[1] == NUMBER:
                    setattr(cls, field, fields.IntField(range_field=True))
        cls.get_fields()

    @classmethod
    def get_table(cls):
        if cls._table is None:
            cls.generate_table()
        return cls._table

    @classmethod
    def get_items(cls):
        return cls.get_table().items

    @classmethod
    def get_hash(cls):
        return cls.get_table().hash_attr[0]

    @classmethod
    def get_range(cls):
        if cls.get_table().range_attr:
            return cls.get_table().range_attr[0]
        else:
            return None

    table = ClassProperty(get_table)
    items = ClassProperty(get_items)
    hash = ClassProperty(get_hash)
    range = ClassProperty(get_range)

    @property
    def hk(self):
        if self.key:
            return self.key[self._hash_field]
        else:
            return None

    @property
    def rk(self):
        if self.key:
            if self._range_field is not None:
                return self.key[self._range_field]
        else:
            return None

    @classmethod
    def get_table_name(cls):
        return cls.__name__

    def save(self):
        if not self.key:
            created, item = self.get_table().items.create(item=self.to_db())
            if created:
                self.to_python(item)
            if not created:
                raise RuntimeError('Item not created')
        else:
            self.get_table().items.put(item=self.to_db())

    def to_db(self, data=None):
        item = super(Model, self).to_db(data)
        item.update(self.key)
        return item

    def to_python(self, data):
        self.key = self.get_table().items.get_key_from_item(data)
        return super(Model, self).to_python(data)

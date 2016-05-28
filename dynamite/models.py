from dynamite import fields
from dynamite.defines import STRING, NUMBER
from dynamite.schema import Schema
from dynamite.tables import Table
from dynamite.items import TableItems
from dynamite.utils import ClassProperty


class Model(Schema):
    _table = None
    _items = None
    _key = None
    read_capacity_units = 5
    write_capacity_units = 5
    Table = Table
    Items = TableItems

    # ignore class properties in get_fields
    _ignore_elems = set(['table', 'items', 'hash', 'range'])

    hash_generator = None

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)
        if self.__class__._table is None:
            self.generate_table()
        self.generate_key()
        self._update_state(**kwargs)

    @classmethod
    def generate_table(cls):
        cls._get_fields()
        range_attr = None
        if cls._range_field is not None:
            range_attr = [cls._range_field, cls._fields_[cls._range_field].db_type]
        hash_attr = None
        if cls._hash_field is not None:
            hash_attr = [cls._hash_field, cls._fields_[cls._hash_field].db_type]
        cls._table = cls.Table(
            name=cls.get_table_name(),
            hash_attr=hash_attr,
            range_attr=range_attr,
            read_capacity_units=cls.read_capacity_units,
            write_capacity_units=cls.write_capacity_units,
            hash_generator=cls.hash_generator,
            items=cls.Items,
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
        cls._get_fields()

    @classmethod
    def get_table(cls):
        if cls._table is None:
            cls.generate_table()
        return cls._table

    @classmethod
    def get_items(cls):
        if cls._items is None:
            cls._items = cls.get_table().items
        return cls._items

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
        return self.key.get(self._hash_field)

    @property
    def rk(self):
        if self._range_field is not None:
           return self.key.get(self._range_field)

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.key)

    @classmethod
    def get_table_name(cls):
        return cls.__name__

    def save(self):
        if not self.hk:
            created, item = self.get_table().items.create(item=self.to_db())
            if created:
                self.to_python(item)
            else:
                raise RuntimeError('Item not created')
        else:
            self.get_table().items.put(self.to_db())

    def generate_key(self):
        self._key = self.table.items.get_key_from_item(self.to_db())

    @property
    def key(self):
        self.generate_key()
        return self._key

    def to_python(self, data):
        super(Model, self).to_python(data)
        self.generate_key()
        return self

    @classmethod
    def to_python_cls(cls, data):
        if data is None:
            return None
        instance = super(Model, cls).to_python_cls(data)
        instance.generate_key()
        return instance

    @classmethod
    def get(cls, **key):
        return cls.to_python_cls(cls.items.get(item=key))

    @classmethod
    def delete(cls, **key):
        return cls.items.delete(item=key)

    @classmethod
    def scan(cls, **params):
        for item in cls.items.scan(**params):
            yield cls.to_python_cls(item)

    @classmethod
    def query(cls, **params):
        for item in cls.items.query(**params):
            yield cls.to_python_cls(item)

    @classmethod
    def all(cls):
        for item in cls.items.all():
            yield cls.to_python_cls(item)


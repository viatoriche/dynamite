from dynamite.schema import Schema
from dynamite.tables import Table


class Model(Schema):
    _table = None
    read_capacity_units = 5
    write_capacity_units = 5

    # @classmethod
    hash_generator = None

    def __init__(self, **kwargs):
        self.key = kwargs.pop('key', {})
        super(Model, self).__init__(**kwargs)
        if self.__class__._table is None:
            self.init_table()

    @classmethod
    def init_table(cls):
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

    @classmethod
    def table(cls):
        if cls._table is None:
            cls.init_table()
        return cls._table

    @classmethod
    def items(cls):
        return cls.table().items

    @classmethod
    def get_table_name(cls):
        return cls.__name__

    def save(self):
        if not self.key:
            created, item = self.table().items.create(item=self.to_db())
            if created:
                self.to_python(item)
            if not created:
                raise RuntimeError('Item not created')
        else:
            self.table().items.put(item=self.to_db())

    def to_db(self, data=None):
        item = super(Model, self).to_db(data)
        item.update(self.key)
        return item

    def to_python(self, data):
        self.key = self.table().items.get_key_from_item(data)
        return super(Model, self).to_python(data)

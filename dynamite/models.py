from schema import Schema
import tables

class Model(Schema):

    Table = None

    hash_generator = None

    def __init__(self, **kwargs):
        self.key = kwargs.pop('key', {})
        self.hash_generator = kwargs.pop('hash_generator', self.hash_generator)
        if self.Table is None:
            self.Table = kwargs.pop('Table', None)
        self.build_table()
        if self.hash_generator is not None:
            self.Table.hash_generator = self.hash_generator
        super(Model, self).__init__(**kwargs)
        if self._range_field is not None:
            self.Table.range_attr = [self._range_field, self.fields[self._range_field].db_type]
        if self._hash_field is not None:
            self.Table.hash_attr = [self._hash_field, self.fields[self._hash_field].db_type]
        self.table = self.Table()
        self.items = self.table.items

    def build_table(self):
        if self.Table is None:
            self.Table = tables.build_table(
                self.get_table_name(),
            )

    def get_table_name(self):
        return '{}Table'.format(self.__class__.__name__)

    def save(self):
        if not self.key:
            created, item = self.items.create(item=self.to_db())
            if created:
                self.to_python(item)
            if not created:
                raise RuntimeError('Item not created')
        else:
            self.items.put(item=self.to_db())

    def to_db(self, data=None):
        item = super(Model, self).to_db(data)
        item.update(self.key)
        return item

    def to_python(self, data):
        self.key = self.items.get_key_from_item(data)
        return super(Model, self).to_python(data)

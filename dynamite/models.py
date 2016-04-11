from schema import Schema
import tables

class Model(Schema):

    Table = None

    def __init__(self, **kwargs):
        self.key = kwargs.pop('key', {})
        if self.Table is None:
            self.Table = kwargs.pop('Table', None)
        self.build_table()
        super(Model, self).__init__(**kwargs)
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
        else:
            self.items.put(item=self.to_db())

    def to_db(self, data=None):
        item = super(Model, self).to_db(data)
        item.update(self.key)
        return item

    def to_python(self, data):
        self.key = self.items.get_key_from_item(data)
        return super(Model, self).to_python(data)

    def query(self, *args, **kwargs):
        return self.items.query(*args,  **kwargs)

    def scan(self, *args, **kwargs):
        return self.items.scan(*args,  **kwargs)

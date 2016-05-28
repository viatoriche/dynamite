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
        key = {}
        key_value = self.get_hash_from_item(item)
        if key_value is not None:
            key[self.table.get_hash_name()] = key_value
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
        item = response.get('Item', None)
        return item

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
        for item in items:
            yield item

    def query(self, **options):
        response = self.table.query(**options)
        items = response.get('Items', [])
        for item in items:
            yield item

    def all(self):
        response = self.table.scan()
        items = response.get('Items', [])
        for item in items:
            yield item

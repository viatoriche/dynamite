import boto3
from dynamite.config import dynamite_options
from dynamite.patterns import Singleton

if not dynamite_options.CONNECTION.region_name:
    dynamite_options.CONNECTION.region_name = 'us-east-1'

class Connection(Singleton):

    _db = None
    config = None

    def __init__(self, **options):
        if self.config is None:
            self.config = {}
            self.update_config(**options)
        if self._db is None:
            self._db = self.get_resource()

    def update_config(self, **options):
        self.config.update(dynamite_options['CONNECTION'])
        self.config.update(options)

    def get_resource(self):
        return boto3.resource('dynamodb', **self.config)

    def rebuild(self, **options):
        self.update_config(**options)
        self._db = self.get_resource()

    def __getattr__(self, item):
        return getattr(self._db, item)

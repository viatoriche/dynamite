import boto3
from dynamite.config import Config

class ConnectionOption(object):

    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value

class Connection(object):

    region_name = ConnectionOption('us-east-1')

    def __init__(self):
        config = Config().get(
            'DEFAULT_CONNECTION', {}
        ).copy()
        config.update(self.options)
        self.config = config
        self._db = boto3.resource('dynamodb', **self.config)

    def __getattr__(self, item):
        return getattr(self._db, item)

    @classmethod
    def get_options(cls):
        options = {}
        for elem in dir(cls):
            attr = getattr(cls, elem)
            if isinstance(attr, ConnectionOption):
                options[elem] = attr.value
        return options

    @property
    def options(self):
        return self.get_options()

def build_connection(classname, **kwargs):
    kwargs = {k: ConnectionOption(kwargs[k]) for k in kwargs}
    return type(classname, (Connection, ), kwargs)
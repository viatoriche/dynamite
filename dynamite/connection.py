import boto3

class Connection(object):

    def __init__(self, **options):
        options['region_name'] = options.pop('region_name', 'us-east-1')
        self._db = boto3.resource('dynamodb', **options)

    def __getattr__(self, item):
        return getattr(self._db, item)

# Dynamite - DynamoDB ORM

based on boto3

## Usage

```
from dynamite import Model
from dynamite.fields import MapField

class User(Model):
    credentials = MapField()

```
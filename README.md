# Dynamite - DynamoDB ORM

based on boto3

## Usage

```
from dynamite import Table

class User(Table):
    name = 'dynamite.User'
    endpoint_url = 'http://localhost:8000'


# will be create a new new table if does not exists
user_table = User()

created, user_item = user_table.items.create()

user_item = user_table.items.get(user_item)

```
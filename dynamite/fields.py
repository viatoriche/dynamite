import base64

try:
    import cPickle as pickle
except ImportError:
    import pickle

from dynamite.schema import Schema
from dynamite import defines
import six


class BaseField(object):
    default = None
    python_type = None
    db_type = None

    def __init__(self, default=None, range_field=False, hash_field=False, db_type=None, name=None):
        if default is None and self.python_type is not None:
            default = self.python_type()
        if db_type is not None:
            self.db_type = db_type
        if self.db_type is not None:
            self._range = range_field
            self._hash = hash_field
        self.default = default
        self.name = name

    def validate(self, value):
        if self.python_type is not None:
            if not isinstance(value, self.python_type):
                raise SchemaValidationError(value, self.python_type)

    def to_python(self, value):
        return value

    def to_db(self, value):
        return value


class SchemaValidationError(ValueError):
    def __init__(self, value, valid_type):
        self.message = 'Type {} of value is not a {}'.format(type(value), valid_type)

    def __str__(self):
        return self.message


class UnicodeField(BaseField):
    python_type = six.text_type
    db_type = defines.STRING

    def __init__(self, encoding='utf8', **kwargs):
        self.encoding = encoding
        super(UnicodeField, self).__init__(**kwargs)

    def to_python(self, value):
        if not isinstance(value, six.text_type):
            return value.decode(self.encoding)
        else:
            return value


class BooleanField(BaseField):
    python_type = bool
    db_type = defines.BINARY


class DynamoStringField(UnicodeField):
    def validate(self, value):
        if not isinstance(value, (six.text_type, six.binary_type)):
            raise SchemaValidationError(value, (six.text_type, six.binary_type))


class DynamoNumberField(BaseField):
    python_type = int
    db_type = defines.NUMBER

    def validate(self, value):
        str_value = str(value)
        if not str_value.isdigit():
            raise SchemaValidationError(value, self.python_type)

    def to_python(self, value):
        if not isinstance(value, self.python_type):
            value = self.python_type(value)
        return value


class BinaryField(BaseField):
    python_type = six.binary_type
    db_type = defines.STRING

    def __init__(self, encoding='utf8', **kwargs):
        self.encoding = encoding
        super(BinaryField, self).__init__(**kwargs)

    def to_python(self, value):
        # Check for {Binary} field
        if hasattr(value, 'value'):
            value = value.value
        if not isinstance(value, six.binary_type):
            return value.encode(self.encoding)
        else:
            return value

    def to_db(self, value):
        return value.decode(encoding='utf8')

StrField = BinaryField

class PickleField(BaseField):
    db_type = defines.BINARY
    python_type = object

    def to_python(self, value):
        if hasattr(value, 'value'):
            value = value.value
        if isinstance(value, six.text_type):
            value = value.encode()
        return pickle.loads(value)

    def to_db(self, value):
        return pickle.dumps(value)


class IntField(BaseField):
    python_type = int
    db_type = defines.NUMBER


class FloatField(BaseField):
    python_type = float
    db_type = defines.NUMBER


class LongField(BaseField):
    python_type = int
    db_type = defines.NUMBER


class DictField(BaseField):
    python_type = dict
    db_type = defines.MAP


class ListField(BaseField):
    python_type = list
    db_type = defines.LIST


class Base64Field(BaseField):
    python_type = six.binary_type
    db_type = defines.STRING

    def to_db(self, value):
        return base64.b64encode(value)

    def to_python(self, value):
        if hasattr(value, 'value'):
            value = value.value
        if isinstance(value, six.text_type):
            value = value.encode()
        return base64.b64decode(value)


class SchemaField(BaseField):
    python_type = Schema
    db_type = defines.MAP

    def __init__(self, SchemaClass, **kwargs):
        self.python_type = SchemaClass
        super(SchemaField, self).__init__(**kwargs)

    def to_db(self, value):
        return value.to_db()

    def to_python(self, data):
        """Data may be dict or Schema"""

        if isinstance(data, dict):
            value = self.python_type()
            for key in data:
                value._set_state(key, value._fields[key].to_python(data[key]))
        elif isinstance(data, Schema):
            value = data
        else:
            raise ValueError('Unknown data type: {}'.format(data))

        return value

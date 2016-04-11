import base64
import cPickle as pickle

from dynamite.schema import Schema


class BaseField(object):
    default = None
    type = None

    def __init__(self, default=None):
        if default is None and self.type is not None:
            default = self.type()
        self.default = default

    def validate(self, value):
        if type is not None:
            if not isinstance(value, self.type):
                raise SchemaValidationError(value, self.type)

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
    type = unicode

    def __init__(self, encoding='utf8', **kwargs):
        self.encoding = encoding
        super(UnicodeField, self).__init__(**kwargs)

    def to_python(self, value):
        if not isinstance(value, unicode):
            return value.decode(self.encoding)
        else:
            return value


class StrField(BaseField):
    type = str

    def __init__(self, encoding='utf8', **kwargs):
        self.encoding = encoding
        super(StrField, self).__init__(**kwargs)

    def to_python(self, value):
        if not isinstance(value, str):
            return value.encode(self.encoding)
        else:
            return value

    def to_db(self, value):
        return value.decode(encoding='utf8')


class PickleField(BaseField):
    type = object

    def to_python(self, value):
        return pickle.loads(value)

    def to_db(self, value):
        return pickle.dumps(value)


class IntField(BaseField):
    type = int


class FloatField(BaseField):
    type = float


class LongField(BaseField):
    type = long


class DictField(BaseField):
    type = dict


class ListField(BaseField):
    type = list


class Base64Field(BaseField):

    type = str

    def to_db(self, value):
        return base64.b64encode(value)

    def to_python(self, value):
        return base64.b64decode(value)


class SchemaField(BaseField):
    type = Schema

    def __init__(self, SchemaClass, **kwargs):
        self.type = SchemaClass
        super(SchemaField, self).__init__(**kwargs)

    def to_db(self, value):
        return value.to_db()

    def to_python(self, data):
        """Data may be dict or Schema"""

        if isinstance(data, dict):
            value = self.type()
            for key in data:
                value.set_state(key, value.fields[key].to_python(data[key]))
        elif isinstance(data, Schema):
            value = data
        else:
            raise ValueError('Unknown data type: {}'.format(data))

        return value
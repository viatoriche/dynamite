import cPickle as pickle
import base64


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
    default = u''

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
    default = ''

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

class Schema(object):
    _state = None
    _fields = None
    defaults_to_python = False

    def __init__(self):
        self._state = {}
        self._fields = self.get_fields()
        self.init_state()

    def init_state(self):
        for field in self.fields:
            self.set_state(field, self.fields[field].default)

    def set_state(self, name, value):
        self.fields[name].validate(value)
        self.state[name] = value
        return value

    def get_state(self, name):
        value = self.state[name]
        self.fields[name].validate(value)
        return value

    @classmethod
    def get_fields(cls):
        return {elem: getattr(cls, elem) for elem in dir(cls) if isinstance(getattr(cls, elem), BaseField)}

    @property
    def fields(self):
        return self._fields

    @property
    def state(self):
        return self._state

    def __setattr__(self, key, value):
        if self.fields is not None and key in self.fields:
            self.set_state(key, value)
        else:
            super(Schema, self).__setattr__(key, value)

    def __getattribute__(self, item):
        fields = super(Schema, self).__getattribute__('_fields')
        if fields is not None and item in fields:
            return self.get_state(item)
        return super(Schema, self).__getattribute__(item)

    def to_db(self, data=None):
        if data is None:
            data = self.state
        result = {}
        for field in self.fields:
            result[field] = self.fields[field].to_db(data[field])
        return result

    def to_python(self, data):
        for field in self.fields:
            value = None
            if field in data:
                value = self.fields[field].to_python(data[field])
            elif self.defaults_to_python:
                value = self.fields[field].default
            if value is not None:
                self.set_state(field, value)
        return self


class SchemaField(BaseField):
    type = Schema

    def __init__(self, SchemaClass, **kwargs):
        self.type = SchemaClass
        super(SchemaField, self).__init__(**kwargs)

    def to_db(self, value):
        return value.to_db()

    def to_python(self, data):
        """Data may be dict or Schema"""

        value = data
        if isinstance(data, dict):
            value = self.type()
        elif not isinstance(data, Schema):
            raise ValueError('Unsupported type: {}'.format(type(data)))

        return value

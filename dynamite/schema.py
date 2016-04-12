class Schema(object):
    _state = None
    _fields = None
    defaults_to_python = False

    def __init__(self, **kwargs):
        self._state = {}
        self._fields = self.get_fields()
        self.init_state()
        for key in kwargs:
            if key in self.fields:
                self.set_state(key, kwargs[key])
        self._range_field = None
        self._hash_field = None
        for field in self.fields:
            if self.fields[field]._range:
                self._range_field = field
            if self.fields[field]._hash:
                self._hash_field = field

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
        from dynamite.fields import BaseField
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



class Schema(object):
    _state = None
    _fields = None
    _range_field = None
    _hash_field = None
    _ignore_elems = set([])

    @classmethod
    def _ignore_elem(cls, elem, need_class):
        return elem.startswith('_') or elem in cls._ignore_elems or getattr(Schema, elem, None) is not None or not isinstance(getattr(cls, elem), need_class)

    defaults_to_python = False

    def __init__(self, **kwargs):
        self._state = {}
        if self.__class__._fields is None:
            self.get_fields()
        self.init_state()
        for key in kwargs:
            if key in self.fields:
                self.set_state(key, kwargs[key])

    def get_default_value(self, field):
        value = self.fields[field].default
        if callable(value):
            value = value()
        return value


    def init_state(self):
        for field in self.fields:
            value = self.get_default_value(field)
            self.set_state(field, value)

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

        elems = [elem for elem in dir(cls) if not cls._ignore_elem(elem, BaseField)]
        cls._fields = {elem: getattr(cls, elem) for elem in elems}

        for field in cls._fields:
            if cls._fields[field]._range:
                cls._range_field = field
            if cls._fields[field]._hash:
                cls._hash_field = field

    @property
    def fields(self):
        return self.__class__._fields

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

    @classmethod
    def __get_class_attribute__(cls, item):
        return getattr(Schema, item)

    def to_db(self, data=None):
        if data is None:
            data = self.state
        result = {}
        for field in self.fields:
            value = self.fields[field].to_db(data[field])
            # result[field] = value
            if value:
                result[field] = value
        return result

    def to_python(self, data):
        for field in self.fields:
            value = None
            if field in data:
                value = self.fields[field].to_python(data[field])
            elif self.defaults_to_python:
                value = self.get_default_value(field)
            if value is not None:
                self.set_state(field, value)
        return self



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

    def update_state(self, **kwargs):
        self.default_state(**kwargs)
        for key in kwargs:
            if key in self.fields:
                self.set_state(key, kwargs[key])

    def __init__(self, **kwargs):
        self._state = {}
        if self.__class__._fields is None:
            self.get_fields()
        self.update_state(**kwargs)

    def get_default_value(self, field):
        value = self.fields[field].default
        if callable(value):
            value = value()
        return value


    def default_state(self, **kwargs):
        fields = {k: self.fields[k] for k in self.fields if k not in kwargs}
        for field in fields:
            if self.get_state(field) is None:
                value = self.get_default_value(field)
                self.set_state(field, value)

    def set_state(self, name, value):
        self.fields[name].validate(value)
        self.state[name] = value
        return value

    def get_state(self, name):
        if name in self.state:
            value = self.state[name]
            if value is not None:
                self.fields[name].validate(value)
        else:
            value = None
        return value

    def __getitem__(self, item):
        if item in self.fields:
            return self.get_state(item)
        return None

    def __setitem__(self, key, value):
        if key in self.fields:
            self.set_state(key, value)

    def __iter__(self):
        return self.state.__iter__()

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, ', '.join(['{}={}'.format(field, self.fields[field].__class__.__name__) for field in self.fields]))

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
    def to_db_cls(cls, data):
        result = {}
        for field in cls._fields:
            if field in data:
                value = cls._fields[field].to_db(data[field])
            else:
                value = None
            if value:
                result[field] = value
        return result

    def to_db(self):
        return self.to_db_cls(self.state)

    @classmethod
    def to_python_cls(cls, data):
        instance = cls()
        instance.to_python(data)
        return instance

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



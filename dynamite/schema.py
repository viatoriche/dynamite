class Schema(object):
    _state_ = None
    _fields_ = None
    _range_field = None
    _hash_field = None
    _ignore_elems = set([])

    @classmethod
    def _ignore_elem(cls, elem, need_class):
        return elem.startswith('_') or elem in cls._ignore_elems or getattr(Schema, elem,
                                                                            None) is not None or not isinstance(
            getattr(cls, elem), need_class)

    _defaults_to_python = False

    def _update_state(self, **kwargs):
        self._set_default_state(**kwargs)
        for key in kwargs:
            if key in self._fields:
                self._set_state(key, kwargs[key])

    def __init__(self, **kwargs):
        self._state_ = {}
        if self.__class__._fields_ is None:
            self._get_fields()
        self._update_state(**kwargs)

    def _get_default_value(self, field):
        value = self._fields[field].default
        if callable(value):
            value = value()
        return value

    def _set_default_state(self, **kwargs):
        fields = {k: self._fields[k] for k in self._fields if k not in kwargs}
        for field in fields:
            if self._get_state(field) is None:
                value = self._get_default_value(field)
                self._set_state(field, value)

    def _set_state(self, name, value):
        self._fields[name].validate(value)
        self._state[name] = value
        return value

    def _get_state(self, name):
        if name in self._state:
            value = self._state[name]
            if value is not None:
                self._fields[name].validate(value)
        else:
            value = None
        return value

    def __getitem__(self, item):
        if item in self._fields:
            return self._get_state(item)
        return None

    def __setitem__(self, key, value):
        if key in self._fields:
            self._set_state(key, value)

    def __iter__(self):
        return self._state.__iter__()

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, ', '.join(
            ['{}={}'.format(field, self._fields[field].__class__.__name__) for field in self._fields]))

    @classmethod
    def _get_fields(cls):
        from dynamite.fields import BaseField

        elems = [elem for elem in dir(cls) if not cls._ignore_elem(elem, BaseField)]
        cls._fields_ = {elem: getattr(cls, elem) for elem in elems}

        pre_fields = dict(cls._fields_)
        for field in cls._fields_.keys():
            if cls._fields_[field].name is not None:
                pre_fields[cls._fields_[field].name] = cls._fields_[field]
                del pre_fields[field]
        cls._fields_ = pre_fields

        for field in cls._fields_:
            if cls._fields_[field]._range:
                cls._range_field = field
            if cls._fields_[field]._hash:
                cls._hash_field = field

    @property
    def _fields(self):
        return self.__class__._fields_

    @property
    def _state(self):
        return self._state_

    def __setattr__(self, key, value):
        if self._fields is not None and key in self._fields:
            self._set_state(key, value)
        else:
            super(Schema, self).__setattr__(key, value)

    def __getattribute__(self, item):
        fields = super(Schema, self).__getattribute__('_fields_')
        if fields is not None and item in fields:
            return self._get_state(item)
        return super(Schema, self).__getattribute__(item)

    @classmethod
    def to_db_cls(cls, data):
        result = {}
        for field in cls._fields_:
            if field in data:
                value = cls._fields_[field].to_db(data[field])
            else:
                value = None
            if value:
                result[field] = value
        return result

    def to_db(self):
        return self.to_db_cls(self._state)

    @classmethod
    def to_python_cls(cls, data):
        instance = cls()
        instance.to_python(data)
        return instance

    def to_python(self, data):
        for field in self._fields:
            value = None
            if field in data:
                value = self._fields[field].to_python(data[field])
            elif self._defaults_to_python:
                value = self._get_default_value(field)
            if value is not None:
                self._set_state(field, value)
        return self

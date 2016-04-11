class Singleton(object):
    """Singleton class"""

    __instances = {}
    _baseclass = object

    def __new__(cls, *args, **kwargs):
        instance = Singleton.__instances.get(cls)
        if instance is None:
            Singleton.__instances[cls] = cls._baseclass.__new__(cls, *args, **kwargs)
            instance = Singleton.__instances[cls]
        return instance
from dynamite.patterns import Singleton
import addict

class Config(Singleton):

    def __getitem__(self, item):
        if item not in self.data:
            self.data[item] = addict.Dict()
        return self.data[item]

    def __init__(self):
        self.data = addict.Dict()

    def __iter__(self):
        return self.data.__iter__()

    def __setitem__(self, key, value):
        self.data[key] = value

    def __getattr__(self, item):
        return self[item]

    def get(self, item, default=None):
        return self.data.get(item, default)


dynamite_options = Config()

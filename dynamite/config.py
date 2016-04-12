from dynamite.patterns import Singleton
from addict import Dict

class Config(Singleton):
    data = Dict()

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, item):
        return self[item]

    def get(self, item, default=None):
        return self.data.get(item, default)

dynamite_options = Config()

from os.path import dirname, basename, isdir, join
import glob
import importlib


CATEGORIES = ['input', 'output', 'decoder', 'parser', 'filter', 'executor']


class PluginManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls.registries = {
                category: PluginRegistry(category)
                for category in CATEGORIES
            }

        return cls._instance

    def register(self, category, name, cls):
        self.registries[category].register(name, cls)

    def fetch(self, category, name):
        return self.registries[category].fetch(name)


class PluginRegistry:
    def __init__(self, category):
        self.category = category
        self.table = {}

    def register(self, name, cls):
        print(f"register {name}, {cls}")
        self.table[name] = cls

    def fetch(self, name):
        return self.table[name]


def executor_plugin(name):
    def _register(cls):
        PluginManager().register('executor', name, cls)
        return cls
    return _register


def input_plugin(name):
    def _register(cls):
        PluginManager().register('input', name, cls)
        return cls
    return _register


def parser_plugin(name):
    def _register(cls):
        PluginManager().register('parser', name, cls)
        return cls
    return _register


def decoder_plugin(name):
    def _register(cls):
        PluginManager().register('decoder', name, cls)
        return cls
    return _register


def filter_plugin(name):
    def _register(cls):
        PluginManager().register('filter', name, cls)
        return cls
    return _register


def output_plugin(name):
    def _register(cls):
        PluginManager().register('output', name, cls)
        return cls
    return _register


def init_plugins():
    base_dir = join(dirname(__file__), '..')
    print(base_dir)
    mods = glob.glob(join(base_dir, f'plugins/*'))
    mods = [basename(f) for f in mods if isdir(f)]
    for mod in mods:
        importlib.import_module(f'plugins.{mod}')

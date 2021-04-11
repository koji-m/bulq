from os.path import dirname, basename, isdir, join
import glob
import importlib
import logging
import pkg_resources

from bulq.core.plugin_base import (
    BulqRunnerPlugin,
    BulqInputPlugin,
    BulqOutputPlugin,
    BulqDecoderPlugin,
    BulqParserPlugin,
    BulqTransformPlugin,
)
import bulq.plugins


logger = logging.getLogger(__name__)


PLUGIN_CLASS = {
    'input': BulqInputPlugin,
    'output': BulqOutputPlugin,
    'decoder': BulqDecoderPlugin,
    'parser': BulqParserPlugin,
    'transform': BulqTransformPlugin,
    'runner': BulqRunnerPlugin,
}

STANDARD_PLUGINS = [
    'runner-direct', 'runner-dataflow',
    'input-file',
    'output-stdout',
    'decoder-auto-detect', 'decoder-gzip',
    'parser-csv',
]

class PluginManager:
    _instance = None
    _loaded_plugins = []

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def fetch(self, category, conf_section):
        name = conf_section['type']
        if f'{category}-{name}' in STANDARD_PLUGINS:
            distribution = 'bulq'
            is_extra = False
        else:
            distribution = f'bulq-{category}-{name}'
            is_extra = True

        plugin_pkg = pkg_resources.load_entry_point(
            distribution,
            f'bulq.plugins.{category}',
            f'{name}'
        )
        plugin_cls = plugin_pkg.plugin
        self.check_plugin(plugin_cls, category)

        plugin = plugin_cls(conf_section)

        self.__class__._loaded_plugins.append({
            'package': plugin_pkg,
            'instance': plugin,
            'is_extra': is_extra,
        })

        return plugin

    def setup_plugins(self):
        for plugin in self.__class__._loaded_plugins:
            plugin['instance'].setup()

    def loaded_plugin_packages(self):
        return [
            plugin['package'].__name__
            for plugin in self.__class__._loaded_plugins
            if not plugin['is_extra']]

    def loaded_extra_plugins(self):
        return [
            plugin
            for plugin in self.__class__._loaded_plugins
            if plugin['is_extra']]

    @staticmethod
    def check_plugin(plugin_cls, category):
        base_cls = plugin_cls.__bases__[0]
        if base_cls != PLUGIN_CLASS[category]:
            raise Exception(f'invalid plugin type: {base_cls}')

    @staticmethod
    def list_plugins():
        for plugin_type in PLUGIN_CLASS.keys():
            print(f'{plugin_type}:')
            for entry_point in pkg_resources.iter_entry_points(f'bulq.plugins.{plugin_type}'):
                print(f'  {entry_point.name}')

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
    BulqFilterPlugin,
)
import bulq.plugins


logger = logging.getLogger(__name__)


PLUGIN_CLASS = {
    'input': BulqInputPlugin,
    'output': BulqOutputPlugin,
    'decoder': BulqDecoderPlugin,
    'parser': BulqParserPlugin,
    'filter': BulqFilterPlugin,
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

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def fetch(self, category, name):
        if f'{category}-{name}' in STANDARD_PLUGINS:
            distribution = 'bulq'
        else:
            distribution = f'bulq-{category}-{name}'

        plugin_cls = pkg_resources.load_entry_point(
            distribution,
            f'bulq.plugins.{category}',
            f'{name}'
        )
        self.check_plugin(plugin_cls, category)
        return plugin_cls

    @staticmethod
    def check_plugin(plugin_cls, category):
        base_cls = plugin_cls.__bases__[0]
        if base_cls != PLUGIN_CLASS[category]:
            raise Exception(f'invalid plugin type: {base_cls}')


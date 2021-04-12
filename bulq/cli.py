import argparse
import subprocess
import sys
import os
from os.path import dirname
import logging

from jinja2 import Environment, FileSystemLoader
import yaml

from bulq.__version__ import __version__
from bulq.core.pipeline import PipelineBuilder
from bulq.core.plugin import PluginManager
import bulq.log


DEFAULT_RUNNER_CONFIG = {
    'type': 'direct',
    'max_threads': 2,
    'min_output_tasks': 1
}


logger = logging.getLogger(__name__)


class EnvVars:
    def __init__(self, environ):
        self.env = environ

    def __getattr__(self, key):
        return self.env[key]


def get_runner_plugin(conf):
    run_conf = conf.get('run', DEFAULT_RUNNER_CONFIG)
    manager = PluginManager()
    return manager.fetch('runner', run_conf)


def run(args):
    logger.info(f'bulq v{__version__}')
    logger.info('start running bulk load')
    env = Environment(loader=FileSystemLoader('.'))
    conf_template = env.get_template(args.conf_file)
    env_vars = EnvVars(os.environ)
    template_vars = {'env': env_vars}
    conf_rendered = conf_template.render(template_vars)
    conf = yaml.load(conf_rendered, Loader=yaml.FullLoader)

    runner_plugin = get_runner_plugin(conf)
    p_builder = PipelineBuilder(conf)
    p_builder.load_plugins()
    with runner_plugin.pipeline_options() as pipeline_opts:
        pipeline_manager = p_builder.build(pipeline_opts)
        pipeline_manager.run_pipeline()
        logger.info('finished running bulk load')

def plugin_new(args):
    from cookiecutter.main import cookiecutter
    if args.type in ('input', 'output', 'transform', 'parser', 'decoder', 'runner'):
        template = f'{os.path.dirname(os.path.abspath(__file__))}/templates/{args.type}'
        plugin_module = f'bulq_{args.type}_{args.plugin_id}'
        plugin_class = ''.join([t.capitalize() for t in plugin_module.split('_')])
        cookiecutter(
            template,
            no_input=True,
            extra_context={
                'plugin_id': args.plugin_id,
                'plugin_module': plugin_module,
                'plugin_class': plugin_class,
            })
    else:
        logger.error(f'plugin type must be in input|transform|output|parser|decoder|runner')

def plugin_list(args):
    PluginManager.list_plugins()

def main():
    bulq.log.setup()

    parser = argparse.ArgumentParser(description='bulq - extensible ETL tool')
    subparsers = parser.add_subparsers()

    # run sub-command parser
    parser_run = subparsers.add_parser('run', help='see `run -h`')
    parser_run.add_argument('conf_file',
                            type=str,
                            help='config file (default: config.yml)',
                            default='config.yml')
    parser_run.set_defaults(handler=run)

    # plugin sub-command parser
    parser_plugin = subparsers.add_parser('plugin', help='see `plugin -h`')
    parser_plugin_sub = parser_plugin.add_subparsers()
    parser_plugin_new = parser_plugin_sub.add_parser('new', help='see `plugin new -h`')
    parser_plugin_new.add_argument('type',
                                   type=str,
                                   help='plugin type (input|transform|output|parser|decoder|runner)')
    parser_plugin_new.add_argument('plugin_id',
                                   type=str,
                                   help='plugin id(snake_case)')
    parser_plugin_new.set_defaults(handler=plugin_new)

    parser_plugin_list = parser_plugin_sub.add_parser('list', help='see `plugin list -h`')
    parser_plugin_list.set_defaults(handler=plugin_list)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

import argparse
import subprocess
import sys
from os.path import dirname
import logging

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


def get_runner_plugin(conf, extra_packages):
    run_conf = conf.get('run', DEFAULT_RUNNER_CONFIG)
    manager = PluginManager()
    return manager.fetch('runner', run_conf)


def run(args):
    logger.info('start running bulk load')
    with open(args.conf_file, 'r') as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)

    runner_plugin = get_runner_plugin(conf, args.extra_packages)
    p_builder = PipelineBuilder(conf)
    p_builder.load_plugins()
    with runner_plugin.pipeline_options() as pipeline_opts:
        pipeline_manager = p_builder.build(pipeline_opts)
        pipeline_manager.run_pipeline()
        logger.info('finished running bulk load')

def main():
    bulq.log.setup()
    logger.info(f'bulq v{__version__}')

    parser = argparse.ArgumentParser(description='bulq - simple bulk loader')
    subparsers = parser.add_subparsers()

    # run sub-command parser
    parser_run = subparsers.add_parser('run', help='see `run -h`')
    parser_run.add_argument('conf_file',
                            type=str,
                            help='config file (default: config.yml)',
                            default='config.yml')
    parser_run.add_argument('-e',
                            '--extra_packages',
                            type=str,
                            action='append',
                            help='extra packages for runner to use',
                            default=[])
    parser_run.set_defaults(handler=run)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

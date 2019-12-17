import argparse
import subprocess
import sys
from os.path import dirname

import yaml

from bulq.core.plugin import PluginManager, init_plugins
from bulq.core.pipeline import PipelineBuilder
import bulq.plugins


def run(args):
    init_plugins()
    with open(args.conf_file, 'r') as f:
        conf = yaml.load(f)

    p_builder = PipelineBuilder(conf)
    pipeline_manager = p_builder.build()
    pipeline_manager.run_pipeline()


def install_plugin(args):
    print(f'installing {args.package}')
    plugin_dir = dirname(bulq.plugins.__file__)
    res = subprocess.check_output([sys.executable,
                                   '-m', 'pip', 'install',
                                   args.package,
                                   '-t', plugin_dir])
    print(res.decode())


def list_plugins(args):
    init_plugins()
    manager = PluginManager()
    print('----- installed plugins -----')
    for name, klass in manager.fetch_all().items():
        version = ''
        if hasattr(klass, 'VERSION'):
            version = klass.VERSION
        print(name, ':', version)


def main():
    parser = argparse.ArgumentParser(description='bulq - simple bulk loader')
    subparsers = parser.add_subparsers()

    # run sub-command parser
    parser_run = subparsers.add_parser('run', help='see `run -h`')
    parser_run.add_argument('conf_file',
                            type=str,
                            help='config file (default: config.yml)',
                            default='config.yml')
    parser_run.set_defaults(handler=run)

    # pip sub-command parser
    parser_pip = subparsers.add_parser('pip', help='see `pip -h`')
    pip_subparsers = parser_pip.add_subparsers()
    parser_pip_install = pip_subparsers.add_parser(
        'install', help='see `pip install -h`'
    )
    parser_pip_install.add_argument('package',
                                    type=str,
                                    help='package')

    parser_pip_install.set_defaults(handler=install_plugin)

    parser_pip_list = pip_subparsers.add_parser(
        'list', help='see `pip list -h`'
    )

    parser_pip_list.set_defaults(handler=list_plugins)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    print('starting bulq...')
    main()

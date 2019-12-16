import argparse

import yaml

from bulq.core.plugin import PluginManager, init_plugins
from bulq.core.pipeline import PipelineBuilder


def create_input_part(conf):
    in_conf = conf['in']
    plugin = PluginManager().fetch('input', in_conf['type'])
    in_part = plugin(in_conf)

    return in_part


def create_filters_part(p, conf):
    filter_part = p
    filters_conf = conf.get('filters')
    if filters_conf:
        for filter_conf in filters_conf:
            plugin = PluginManager.fetch('filter', filter_conf['type'])
            filter_part = plugin(filter_part, filter_conf)

    return filter_part


def create_output_part(p, conf):
    out_conf = conf['out']
    plugin = PluginManager().fetch('output', out_conf['type'])
    out_part = plugin(p, out_conf)

    return out_part


DEFAULT_EXEC_CONFIG = {
    'max_threads': 2,
    'min_output_tasks': 1
}


def pipeline_options_from(conf):

    exec_conf = conf.get('exec', DEFAULT_EXEC_CONFIG)
    plugin = PluginManager().fetch('executor',
                                   exec_conf.get('type', 'local'))
    exec_plugin = plugin(exec_conf)
    conf_dict = exec_plugin.pipeline_config()

    return conf_dict


def create_pipeline(conf):
    conf_dict = pipeline_options_from(conf)
    conf_dict['setup_file'] = './staging/setup.py'
    builder = PipelineBuilder(conf_dict, conf)

    return builder.build()


def _copy_package():
    import os
    import shutil
    import bulq
    from os.path import dirname

    bulq_dir = dirname(bulq.__file__),
    shutil.copytree(
        bulq_dir,
        bulq_dir + '/staging/bulq'
    )
    shutil.copyfile(
        './setup.py',
        bulq_dir + '/staging/setup.py'
    )


def run(args):
    _copy_package()
    init_plugins()
    with open(args.conf_file, 'r') as f:
        conf = yaml.load(f)

    pipe_line = create_pipeline(conf)
    pipe_line.run()


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

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    print('starting bulq')
    main()

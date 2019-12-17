import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from bulq.core.plugin import PluginManager


DEFAULT_EXEC_CONFIG = {
    'max_threads': 2,
    'min_output_tasks': 1
}


class PipelineBuilder:
    def __init__(self, conf):
        self.conf = conf

    def build(self):
        manager = PluginManager()

        exec_conf = self.conf.get('exec', DEFAULT_EXEC_CONFIG)
        exec_plugin_cls = manager.fetch('executor',
                                        exec_conf.get('type', 'local'))

        in_conf = self.conf['in']
        input_plugin_cls = manager.fetch('input', in_conf['type'])

        filter_plugin_cls = []
        filters_conf = self.conf.get('filters', [])
        if filters_conf:
            for filter_conf in filters_conf:
                filter_plugin_cls.append(
                    manager.fetch('filter', filter_conf['type']))

        out_conf = self.conf['out']
        output_plugin_cls = manager.fetch('output', out_conf['type'])

        loaded_plugins = []
        exec_plugin = exec_plugin_cls(exec_conf)
        pipeline_opts = exec_plugin.pipeline_options()
        loaded_plugins.append(exec_plugin)

        input_plugin = input_plugin_cls(in_conf)
        input_plugin.prepare(pipeline_opts)
        loaded_plugins.append(input_plugin)
        filter_plugins = [plugin(conf) for plugin, conf
                          in zip(filter_plugin_cls, filters_conf)]
        for plg in filter_plugins:
            plg.prepare(pipeline_opts)
            loaded_plugins.append(plg)
        output_plugin = output_plugin_cls(out_conf)
        output_plugin.prepare(pipeline_opts)
        loaded_plugins.append(output_plugin)

        p_options = PipelineOptions.from_dictionary(pipeline_opts)
        p = beam.Pipeline(options=p_options)
        in_part = input_plugin.build(p)
        filter_part = in_part
        for filter in filter_plugins:
            filter_part = filter.build(filter_part)
        output_plugin.build(filter_part)

        return PipelineManager(p, loaded_plugins)


class PipelineManager:
    def __init__(self, pipeline, loaded_plugins):
        self._pipeline = pipeline
        self._loaded_plugins = loaded_plugins

    def run_pipeline(self):
        for plg in self._loaded_plugins:
            plg.setup()

        self._pipeline.run()

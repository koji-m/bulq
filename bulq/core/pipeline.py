import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from bulq.core.plugin import PluginManager


class PipelineBuilder:
    def __init__(self, conf):
        self.conf = conf

    def load_plugins(self):
        manager = PluginManager()

        in_conf = self.conf['in']
        self.input_plugin = manager.fetch('input', in_conf)

        self.filter_plugins = []
        filters_conf = self.conf.get('filters', [])
        if filters_conf:
            for filter_conf in filters_conf:
                self.filter_plugins.append(
                    manager.fetch('filter', filter_conf))

        out_conf = self.conf['out']
        self.output_plugin = manager.fetch('output', out_conf)

    def build(self, pipeline_opts):
        p_options = PipelineOptions.from_dictionary(pipeline_opts)
        p = beam.Pipeline(options=p_options)
        in_part = self.input_plugin.build(p)
        filter_part = in_part
        for filter in self.filter_plugins:
            filter_part = filter.build(filter_part)
        self.output_plugin.build(filter_part)

        return PipelineManager(p)


class PipelineManager:
    def __init__(self, pipeline):
        self._pipeline = pipeline

    def run_pipeline(self):
        PluginManager().setup_plugins()

        result = self._pipeline.run()
        result.wait_until_finish()


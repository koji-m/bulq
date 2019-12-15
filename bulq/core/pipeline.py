import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from bulq.core.plugin import PluginManager


class PipelineBuilder:
    def __init__(self, conf_dict, conf):
        self.options = conf_dict
        self.conf = conf

    def build(self):
        manager = PluginManager()

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

        input_plugin = input_plugin_cls(in_conf)
        input_plugin.prepare(self.options)
        filter_plugins = [plugin(conf) for plugin, conf
                          in zip(filter_plugin_cls, filters_conf)]
        for plg in filter_plugins:
            plg.prepare(self.options)
        output_plugin = output_plugin_cls(out_conf)
        output_plugin.prepare(self.options)

        p_options = PipelineOptions.from_dictionary(self.options)
        p = beam.Pipeline(options=p_options)
        in_part = input_plugin.build(p)
        filter_part = in_part
        for filter in filter_plugins:
            filter_part = filter.build(filter_part)
        output_plugin.build(filter_part)

        return p

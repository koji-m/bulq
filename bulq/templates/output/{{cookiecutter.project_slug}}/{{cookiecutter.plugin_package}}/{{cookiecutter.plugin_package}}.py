import apache_beam as beam

from bulq.core.plugin_base import BulqOutputPlugin


class {{cookiecutter.plugin_class}}(BulqOutputPlugin):
    def __init__(self, conf):
        pass

    def build(self, p):
        return (p
                | 'Output Example' >> beam.Map(print)
               )

    def setup(self):
        pass


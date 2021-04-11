import apache_beam as beam

from bulq.core.plugin import BulqInputPlugin


class {{cookiecutter.plugin_class}}(BulqInputPlugin):
    def __init__(self, conf):
        pass

    def setup(self):
        pass

    def build(self, p):
        pipeline = (
            p
            | 'Example' >> beam.Create([{'data': i} for i in range(10)])
        )
        return pipeline


import apache_beam as beam

from bulq.core.plugin_base import BulqTransformPlugin
from {{cookiecutter.plugin_package}} import ExampleFn


class {{cookiecutter.plugin_class}}(BulqTransformPlugin):
    def __init__(self, conf):
        self.column = conf['column']

    def build(self, p):
        return (p
                | beam.ParDo(ExampleFn(self.column))
               )

    def setup(self):
        pass


import apache_beam as beam

from bulq.core.plugin_base import BulqTransformPlugin


class {{cookiecutter.plugin_class}}(BulqTransformPlugin):
    def __init__(self, conf):
        pass

    def build(self, p):
        return (p
                | 'Transform Example' >> beam.Map(lambda r: {**r, 'example': 'example'})
               )

    def setup(self):
        pass


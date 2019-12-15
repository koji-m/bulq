import apache_beam as beam

from core.plugin import output_plugin


def to_csv_row(dct):
    print(','.join([str(val) for val in dct.values()]))


@output_plugin('stdout')
class BulqOutputStdout:
    def __init__(self, conf):
        pass

    def prepare(self, pipeline_options):
        pass

    def build(self, p):
        return (p | beam.Map(to_csv_row))

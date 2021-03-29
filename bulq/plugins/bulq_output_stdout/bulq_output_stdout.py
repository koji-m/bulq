import apache_beam as beam

from bulq.core.plugin import BulqOutputPlugin


def to_csv_row(dct):
    print(','.join([str(val) for val in dct.values()]))


class BulqOutputStdout(BulqOutputPlugin):
    def __init__(self, conf):
        pass

    def build(self, p):
        return (p | beam.Map(to_csv_row))

    def setup(self):
        pass

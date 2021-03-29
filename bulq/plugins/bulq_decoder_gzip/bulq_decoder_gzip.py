from apache_beam.io.filesystem import CompressionTypes

from bulq.core.plugin import BulqDecoderPlugin


class BulqDecoderGzip(BulqDecoderPlugin):
    def __init__(self, conf_section):
        self._conf_section = conf_section

    def compression_type(self):
        return CompressionTypes.GZIP

    def setup(self):
        pass

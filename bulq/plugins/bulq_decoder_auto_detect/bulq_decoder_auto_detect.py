from apache_beam.io.filesystem import CompressionTypes

from bulq.core.plugin import BulqDecoderPlugin


class BulqDecoderAutoDetect(BulqDecoderPlugin):
    def __init__(self, conf_section):
        self._conf_section = conf_section

    def compression_type(self):
        return CompressionTypes.AUTO

    def setup(self):
        pass

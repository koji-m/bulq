from apache_beam.io.filesystem import CompressionTypes

from bulq.core.plugin import BulqDecoderPlugin


class BulqDecoderAutoDetect(BulqDecoderPlugin):
    @staticmethod
    def compression_type():
        return CompressionTypes.AUTO

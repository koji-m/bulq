from apache_beam.io.filesystem import CompressionTypes

from bulq.core.plugin import BulqDecoderPlugin


class BulqDecoderGzip(BulqDecoderPlugin):
    @staticmethod
    def compression_type():
        return CompressionTypes.GZIP

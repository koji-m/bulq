from apache_beam.io.filesystem import CompressionTypes

from bulq.core.plugin import decoder_plugin


@decoder_plugin('gzip')
class BulqDecoderGzip:
    @staticmethod
    def compression_type():
        return CompressionTypes.GZIP

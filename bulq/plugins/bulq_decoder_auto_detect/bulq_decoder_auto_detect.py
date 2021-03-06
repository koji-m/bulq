from apache_beam.io.filesystem import CompressionTypes

from bulq.core.plugin import decoder_plugin


@decoder_plugin('auto_detect')
class BulqDecoderAutoDetect:
    @staticmethod
    def compression_type():
        return CompressionTypes.AUTO

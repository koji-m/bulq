from apache_beam.io import Read
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filesystem import CompressedFile

from bulq.core.plugin import BulqInputPlugin, PluginManager


class CompressedStream:
    def __init__(self, file):
        self._file = file

    def __iter__(self):
        return self

    def __next__(self):
        line = self._file.readline()
        if line:
            return line
        raise StopIteration()


class FileSource(FileBasedSource):
    def __init__(self, file_pattern, parser, decoder):
        super().__init__(
            file_pattern,
            compression_type=decoder.compression_type())
        self._parser = parser

    def read_records(self, file_name, range_tracker):
        file_ = self.open_file(file_name)
        if isinstance(file_, CompressedFile):
            file_ = CompressedStream(file_)
        self._parser.set_stream(file_)
        for rec in self._parser:
            yield rec


class BulqInputFile(BulqInputPlugin):
    VERSION = '0.0.1'

    def __init__(self, conf):
        self.path_prefix = conf['path_prefix']
        self.decoders = [PluginManager().fetch('decoder', d_conf)
                         for d_conf in conf['decoders']]
        if len(self.decoders) == 0:
            self.decoders.append(
                PluginManager().fetch('decoder', {'type': 'auto_detect'}))

        self.parser = PluginManager().fetch('parser', conf['parser'])

    def build(self, p):
        file_read = (p
                     | Read(FileSource(
                        self.path_prefix + '*',
                        self.parser,
                        self.decoders[0]
                     )))
        return file_read

    def setup(self):
        pass

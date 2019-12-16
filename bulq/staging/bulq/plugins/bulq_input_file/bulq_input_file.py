from apache_beam.io import Read
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filesystem import CompressedFile

from bulq.core.plugin import PluginManager, input_plugin


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
    def __init__(self,
                 file_pattern,
                 decoders_conf,
                 parser_conf,
                 decoder):
        super().__init__(file_pattern,
                         compression_type=decoder.compression_type())
        self._decoders_conf = decoders_conf
        self._parser_conf = parser_conf
        self._parser = PluginManager().fetch(
            'parser',
            self._parser_conf['type']
        )

    def read_records(self, file_name, range_tracker):
        self._file = self.open_file(file_name)
        if isinstance(self._file, CompressedFile):
            self._file = CompressedStream(self._file)
        parser = self._parser(self._file, self._parser_conf)
        for rec in parser:
            yield rec


@input_plugin('file')
class BulqInputFile:
    def __init__(self, conf):
        self.path_prefix = conf['path_prefix']
        self.decoders_conf = conf['decoders']
        self.decoders = [PluginManager().fetch('decoder', d['type'])
                         for d in self.decoders_conf]
        if len(self.decoders) == 0:
            self.decoders.append(
                PluginManager().fetch('decoder', 'auto_detect'))

        self.parser_conf = conf['parser']

    def prepare(self, pipeline_options):
        pass

    def build(self, p):
        file_read = (p
                     | Read(FileSource(
                        self.path_prefix + '*',
                        self.decoders_conf,
                        self.parser_conf,
                        self.decoders[0]
                     )))
        return file_read

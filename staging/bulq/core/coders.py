import typing

import apache_beam as beam


class Coders:
    TABLE = {
        'bytes': 'Bytes',
        'utf8': 'UTF-8',
        'utf_8': 'UTF-8',
    }

    CODERS = {
        'Bytes': beam.coders.coders.BytesCoder,
        'UTF-8': beam.coders.coders.StrUtf8Coder,
    }

    @classmethod
    def get_coder(cls, coder_name):
        coder = cls.CODERS.get(
            (cls.TABLE[coder_name.lower().replace('-', '_')]),
            None)
        if coder:
            return coder()
        else:
            return StrCustomCoder(coder_name)


class StrCustomCoder(beam.coders.coders.Coder):
    def __init__(self, coder_name):
        super().__init__()
        self._coder_name = coder_name

    def encode(self, value):
        return value.encode(self._coder_name)

    def decode(self, value):
        return value.decode(self._coder_name)

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return typing.Any

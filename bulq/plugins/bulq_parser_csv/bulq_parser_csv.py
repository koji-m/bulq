import csv

from datetime import datetime

from bulq.core.plugin import BulqParserPlugin
from bulq.core.coders import Coders


class StreamWithCode:
    def __init__(self, stream, coder):
        self._stream = stream
        self._coder = coder

    def __iter__(self):
        return self

    def __next__(self):
        b = next(self._stream)
        return self._coder.decode(b)


NAME, PROC = (0, 1)


def int_formatter(s):
    if s:
        return int(s)
    else:
        return None


def float_formatter(s):
    if s:
        return float(s)
    else:
        return None


def string_formatter(s):
    return s


def boolean_formatter(s):
    if s:
        return bool(s)
    else:
        return None


def get_timestamp_formatter(f):
    def _timestamp_formatter(s):
        if s:
            return datetime.strptime(s, f)
        else:
            return None
    return _timestamp_formatter


class BulqParserCsv(BulqParserPlugin):
    def __init__(self, stream, parser_conf):
        self.stream = stream
        self.delimiter = parser_conf.get('delimiter', ',')
        self.quote = parser_conf.get('quote', '"')
        self.escape = parser_conf.get('escape', '\\')
        self.skip_header_lines = parser_conf.get('skip_header_lines', 0)
        self.null_string = parser_conf.get('null_string')
        self.quotes_in_quoted_fields = parser_conf.get(
            'quotes_in_quoted_fields')
        self.comment_line_marker = parser_conf.get('comment_line_marker')
        self.allow_optional_columns = parser_conf.get(
            'allow_optional_columns', False)
        self.allow_extra_columns = parser_conf.get(
            'allow_extra_columns', False)
        self.max_quoted_size_limit = parser_conf.get(
            'max_quoted_size_limit', 131072)
        self.stop_on_invalid_record = parser_conf.get(
            'stop_on_invalid_record', False)
        self.default_timezone = parser_conf.get('default_timezone', 'UTC')
        self.default_date = parser_conf.get('default_date', '%Y-%m-%d')
        self.newline = parser_conf.get('newline', 'CRLF')
        self.line_delimiter_recognized = parser_conf.get(
            'line_delimiter_recognized')
        self.charset = parser_conf.get('charset', 'UTF-8')
        self.columns = parser_conf.get('columns')
        self.formatters = self.build_formatters()

        self.coder = Coders.get_coder(self.charset)

        class custom(csv.excel):
            delimiter = self.delimiter
            quotechar = self.quote
            lineterminator = self.line_terminator()
            escapechar = self.escape

        csv.register_dialect('custom', custom)

        self.csv_reader = csv.reader(
            StreamWithCode(stream, self.coder),
            dialect='custom'
        )

    def line_terminator(self):
        # nonsense
        d = {
            'CR': '\r', 'LF': '\n', 'CRLF': '\r\n'
        }
        return d[self.newline]

    def build_formatters(self):
        formatters = []

        for col in self.columns:
            col_t = col['type']
            if col_t == 'timestamp':
                formatters.append(
                    (col['name'],
                     get_timestamp_formatter(
                        col.get('format', self.default_date))))
            elif col_t == 'long':
                formatters.append(
                    (col['name'], int_formatter))
            elif col_t == 'double':
                formatters.append(
                    (col['name'], float_formatter))
            elif col_t == 'string':
                formatters.append(
                    (col['name'], string_formatter))
            elif col_t == 'boolean':
                formatters.append(
                    (col['name'], boolean_formatter))
            else:
                raise Exception('illeagal column type')

        return formatters

    def __iter__(self):
        # skip header rows
        for _ in range(self.skip_header_lines):
            next(self.csv_reader)

        def _proc(csv_reader,
                  row_len,
                  allow_extra_cols,
                  allow_optional_cols,
                  formatters):
            while True:
                try:
                    row = next(csv_reader)
                    num_col = len(row)
                    if num_col > row_len:
                        if allow_extra_cols:
                            del row[row_len - num_col:]
                        else:
                            continue
                    elif num_col < row_len:
                        if allow_optional_cols:
                            row.extend([None] * (row_len - num_col))
                        else:
                            continue
                    try:
                        res = {f[NAME]: f[PROC](v)
                               for f, v in zip(formatters, row)}
                    except Exception as e:
                        if self.stop_on_invalid_record:
                            raise e
                        print(f"skip record: {e}")
                        continue

                    yield res

                except StopIteration:
                    return

        return _proc(
            self.csv_reader,
            len(self.columns),
            self.allow_extra_columns,
            self.allow_optional_columns,
            self.formatters
        )

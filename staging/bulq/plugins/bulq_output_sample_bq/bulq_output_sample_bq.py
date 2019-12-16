import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from bulq.core.plugin import output_plugin


WRITE_DISPOSITION = {
    'append': BigQueryDisposition.WRITE_APPEND,
    'append_direct': BigQueryDisposition.WRITE_APPEND,
    'replace': BigQueryDisposition.WRITE_TRUNCATE,
    'replace_backup': None,
    'delete_in_advance': BigQueryDisposition.WRITE_TRUNCATE
}

TYPES = {
    'boolean': 'BOOLEAN',
    'long': 'INTEGER',
    'double': 'FLOAT',
    'string': 'STRING',
    'timestamp': 'TIMESTAMP'
}


@output_plugin('sample_bq')
class BulqOutputSampleBq:
    def __init__(self, conf):
        self._write_disposition = WRITE_DISPOSITION[conf['mode']]
        self._gcs_temp_location = 'gs://' + conf['gcs_bucket']
        self._project = conf['project']
        self._dataset = conf['dataset']
        self._table = conf['table']
        self._columns = conf['column_options']

    def prepare(self, pipeline_options):
        pipeline_options['experiments'] = 'use_beam_bq_sink'

    def build(self, p):
        schema = [
            f"{c['name']}:{TYPES[c['type']]}" for c in self._columns
        ]
        return (p
                | beam.Map(
                    lambda rec: {k: str(v) for k, v in rec.items()})
                | WriteToBigQuery(
                    project=self._project,
                    dataset=self._dataset,
                    table=self._table,
                    schema=','.join(schema),
                    write_disposition=self._write_disposition,
                    custom_gcs_temp_location=self._gcs_temp_location))

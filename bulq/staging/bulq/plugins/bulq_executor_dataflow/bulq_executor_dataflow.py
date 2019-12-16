from bulq.core.plugin import executor_plugin


@executor_plugin('dataflow')
class BulqExecutorDataflow:
    def __init__(self, conf):
        self._conf = conf
        self._pipeline_conf = {
            'runner': 'DataflowRunner',
            'project': self._conf.get('project', None),
            'job_name': self._conf.get('job_name', None),
            'staging_location': self._conf.get('staging_location', None),
            'temp_location': self._conf.get('temp_location', None),
        }

    def pipeline_config(self):
        return self._pipeline_conf

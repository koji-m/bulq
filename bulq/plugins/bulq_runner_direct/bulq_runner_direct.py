from bulq.core.plugin_base import BulqRunnerPlugin


class DirectRunnerContext:
    def __init__(self, pipeline_options):
        self._pipeline_options = pipeline_options

    def __enter__(self):
        return self._pipeline_options

    def __exit__(self, exc_type, exc_value, traceback):
        pass



class BulqRunnerDirect(BulqRunnerPlugin):
    def __init__(self, conf):
        self._conf = conf
        self._max_threads = self._conf['max_threads']
        self._pipeline_options = {
            'runner': 'DirectRunner',
            'direct_num_workers': self._max_threads
        }

    def pipeline_options(self):
        return DirectRunnerContext(self._pipeline_options)

    def setup(self):
        pass

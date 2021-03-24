from bulq.core.plugin_base import BulqRunnerPlugin


class BulqRunnerDirect(BulqRunnerPlugin):
    def __init__(self, conf):
        self._conf = conf
        self._max_threads = self._conf['max_threads']
        self._pipeline_conf = {
            'runner': 'DirectRunner',
            'direct_num_workers': self._max_threads
        }

    def pipeline_options(self):
        return self._pipeline_conf

    def setup(self):
        pass

from os.path import dirname, isdir
import shutil

import bulq
from bulq.plugins import bulq_runner_dataflow
from bulq.core.plugin import BulqRunnerPlugin


DF_SETUPFILE = 'dataflow_setup.py'
WORKING_DIR = './.bulq_dataflow/'


class BulqRunnerDataflow(BulqRunnerPlugin):
    def __init__(self, conf):
        self._base_dir = dirname(bulq_runner_dataflow.__file__) + '/'
        self._staging_dir = self._base_dir + '.staging/'
        self._conf = conf
        self._pipeline_options = {
            'runner': 'DataflowRunner',
            'project': self._conf.get('project', None),
            'job_name': self._conf.get('job_name', None),
            'staging_location': self._conf.get('staging_location', None),
            'temp_location': self._conf.get('temp_location', None),
            'setup_file': self._staging_dir + 'setup.py'
        }

    def pipeline_options(self):
        return self._pipeline_options

    def setup(self):
        bulq_dir = dirname(bulq.__file__)
        if isdir(self._staging_dir):
            shutil.rmtree(self._staging_dir)
        if isdir(WORKING_DIR):
            shutil.rmtree(WORKING_DIR)
        shutil.copytree(
            bulq_dir,
            WORKING_DIR + 'bulq'
        )
        shutil.copytree(
            WORKING_DIR + 'bulq',
            self._staging_dir + 'bulq'
        )
        shutil.copyfile(
            self._base_dir + DF_SETUPFILE,
            self._staging_dir + 'setup.py'
        )

import glob
import os
from os.path import dirname, isdir
import pathlib
import pickle
import shutil
import subprocess
import sys
import tempfile

import bulq
from bulq.core.plugin import BulqRunnerPlugin, PluginManager
from bulq.plugins import bulq_runner_dataflow


DF_SETUPFILE = 'dataflow_setup.py'
LOADED_PLUGIN_PACKAGE_FILE = 'loaded_plugin_packages.pickle'


class DataflowRunnerContext:
    def __init__(self, pipeline_options):
        self._pipeline_options = pipeline_options

    def __enter__(self):
        self.tmp_dir = tempfile.mkdtemp() + '/'

        bulq_dir = dirname(bulq.__file__)
        shutil.copytree(
            bulq_dir,
            self.tmp_dir + 'bulq'
        )
        
        setup_file = self.tmp_dir + 'setup.py'
        shutil.copyfile(
            dirname(bulq_runner_dataflow.__file__) + '/' + DF_SETUPFILE,
            setup_file
        )

        self._pipeline_options['setup_file'] = setup_file

        loaded_plugin_packages = PluginManager().loaded_plugin_packages()
        with open(self.tmp_dir + LOADED_PLUGIN_PACKAGE_FILE, 'wb') as f:
            pickle.dump(loaded_plugin_packages, f)

        try:
            cwd = os.getcwd()
            extra_packages = self._build_extra_packages()
        except subprocess.CalledProcessError as e:
            shutil.rmtree(self.tmp_dir)
            raise e
        finally:
            os.chdir(cwd)

        self._pipeline_options['extra_packages'] = extra_packages

        return self._pipeline_options

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.tmp_dir)

    def _build_extra_packages(self):
        extra_plugins = PluginManager().loaded_extra_plugins()
        if len(extra_plugins) < 1:
            return

        extra_staging_dir = self.tmp_dir + '/extra_packages'
        os.mkdir(extra_staging_dir)
        for plugin in extra_plugins:
            pkg_dir = pathlib.Path(dirname(plugin['package'].__file__))
            pkg_root = pkg_dir.parent
            build_cmd = [
                sys.executable,
                str(pkg_root / 'setup.py'),
                'sdist', '--dist-dir', extra_staging_dir]
            os.chdir(pkg_root)
            subprocess.check_call(build_cmd)

        return glob.glob(extra_staging_dir + '/*.tar.gz')


class BulqRunnerDataflow(BulqRunnerPlugin):
    def __init__(self, conf):
        self._conf = conf
        self._pipeline_options = {
            'runner': 'DataflowRunner',
            'project': self._conf.get('project', None),
            'region': self._conf.get('region', None),
            'job_name': self._conf.get('job_name', None),
            'staging_location': self._conf.get('staging_location', None),
            'temp_location': self._conf.get('temp_location', None),
        }

    def pipeline_options(self):
        return DataflowRunnerContext(self._pipeline_options)

    def setup(self):
        pass

import pickle
import setuptools


LOADED_PLUGIN_PACKAGE_FILE = 'loaded_plugin_packages.pickle'
CORE_PACKAGES = ['bulq', 'bulq.core', 'bulq.plugins']

with open(LOADED_PLUGIN_PACKAGE_FILE, 'rb') as f: 
    loaded_plugin_packages = pickle.load(f)

setuptools.setup(
    name='bulq_dist',
    version='0.0.1',
    install_requires=[],
    data_files=[('', [LOADED_PLUGIN_PACKAGE_FILE])],
    packages=CORE_PACKAGES + loaded_plugin_packages
)

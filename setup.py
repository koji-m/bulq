import os
import setuptools

package_name = 'bulq'
project_root = os.path.abspath(os.path.dirname(__file__))
res = {}

with open(os.path.join(project_root, package_name, '__version__.py')) as f:
    exec(f.read(), {}, res)

setuptools.setup(
    name='bulq',
    version=res['__version__'],
    install_requires=[
        'apache-beam[gcp]',
        'Jinja2',
        'cookiecutter'],
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            f'bulq = {package_name}.cli:main'
        ],
        'bulq.plugins.runner': [
            f'direct = {package_name}.plugins.bulq_runner_direct',
            f'dataflow = {package_name}.plugins.bulq_runner_dataflow',
        ],
        'bulq.plugins.input': [
            f'file = {package_name}.plugins.bulq_input_file',
        ],
        'bulq.plugins.output': [
            f'stdout = {package_name}.plugins.bulq_output_stdout',
        ],
        'bulq.plugins.decoder': [
            f'auto_detect = {package_name}.plugins.bulq_decoder_auto_detect',
            f'gzip = {package_name}.plugins.bulq_decoder_gzip',
        ],
        'bulq.plugins.parser': [
            f'csv = {package_name}.plugins.bulq_parser_csv',
        ],
    }
)

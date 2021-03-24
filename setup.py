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
    install_requires=['apache-beam[gcp]'],
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            f'bulq = {package_name}.cli:main'
        ],
        'bulq.plugins.runner': [
            f'direct = {package_name}.plugins.bulq_runner_direct.bulq_runner_direct:BulqRunnerDirect',
            f'dataflow = {package_name}.plugins.bulq_runner_dataflow.bulq_runner_dataflow:BulqRunnerDataflow',
        ],
        'bulq.plugins.input': [
            f'file = {package_name}.plugins.bulq_input_file.bulq_input_file:BulqInputFile',
        ],
        'bulq.plugins.output': [
            f'stdout = {package_name}.plugins.bulq_output_stdout.bulq_output_stdout:BulqOutputStdout',
        ],
        'bulq.plugins.decoder': [
            f'auto_detect = {package_name}.plugins.bulq_decoder_auto_detect.bulq_decoder_auto_detect:BulqDecoderAutoDetect',
            f'gzip = {package_name}.plugins.bulq_decoder_gzip.bulq_decoder_gzip:BulqDecoderGzip',
        ],
        'bulq.plugins.parser': [
            f'csv = {package_name}.plugins.bulq_parser_csv.bulq_parser_csv:BulqParserCsv',
        ],
    }
)

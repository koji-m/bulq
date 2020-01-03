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
        ]
    }
)

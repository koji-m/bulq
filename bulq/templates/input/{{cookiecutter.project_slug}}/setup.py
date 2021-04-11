import setuptools


setuptools.setup(
    name='{{cookiecutter.project_slug}}',
    version='0.0.1',
    install_requires=[],
    packages=setuptools.find_packages(),
    entry_points={
        'bulq.plugins.input': [
            f'{{cookiecutter.plugin_id}} = {{cookiecutter.plugin_module}}',
        ],
    }
)


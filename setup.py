import setuptools


setuptools.setup(
    name='bulq',
    version='0.0.1',
    install_requires=['apache-beam[gcp]'],
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'bulq = bulq.cli:main'
        ]
    }
)

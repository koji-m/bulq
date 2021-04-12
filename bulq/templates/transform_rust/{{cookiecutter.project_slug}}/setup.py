from setuptools import setup
from setuptools_rust import RustExtension


setup(
    name="{{cookiecutter.project_slug}}",
    version="0.1.0",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Rust",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
    ],
    packages=["{{cookiecutter.plugin_package}}"],
    rust_extensions=[RustExtension("{{cookiecutter.plugin_package}}.{{cookiecutter.plugin_module}}_inner", "Cargo.toml", debug=False)],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'bulq.plugins.transform': [
            f'{{cookiecutter.plugin_id}} = {{cookiecutter.plugin_module}}',
        ],
    }
)

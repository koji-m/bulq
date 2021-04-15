# bulq

**Extensible ETL tool.**

You can create a data pipeline simply by defining input (data extraction source), transform, and output (data load destination) in a YAML file. You can also increase the number of combinations of input, transform, and output by adding plugins.

# Features

- **No programming is required** - The pipeline configuration is defined in YAML. 
- **Portable and Scalable** - The pipeline defined in YAML is converted to the pipeline of Apache Beam and can be executed various runners.
- **Can be installed with pip** - bulq is a CLI tool written in Python.
- **Various combinations of ETL** - Build a data pipeline by combining plugins.
- **Extensible architecture** - Plugins can be built by Apache Beam API for Python.

# Install

```shell
$ pip install git+https://github.com/koji-m/bulq
```

# Usage

### run ETL

```shell
$ bulq run your_config.yml
```

### check installed plugins

```shell
$ bulq plugin list
```

### install third-party plugin

```shell
# install output plugin named `bulq-output-bigquery`
$ pip install git+https://github.com/koji-m/bulq-output-bigquery
```

### generate plugin template

```shell
# generate an input plugin tempalte named `example`
$ bulq plugin new input example
```


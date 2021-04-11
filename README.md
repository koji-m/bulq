# bulq

Extensible ETL tool.

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


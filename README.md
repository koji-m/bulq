# bulq

Bulk data loader.

# Install

```shell
$ git clone https://github.com/koji-m/bulq.git
$ docker build -t bulq .
```

# Usage

```shell
$ docker run --rm -it -v ~/.config/gcloud:/root/.config/gcloud bulq bash

# inside docker container
$ bulq run your_config.yml
```


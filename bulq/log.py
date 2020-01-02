import os
import logging
import logging.config

import yaml


CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    'log.yml'
)


def setup(config_path=CONFIG_PATH, log_level=None):
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.load(f)
            if log_level:
                config['root']['level'] = log_level.upper()
        logging.config.dictConfig(config)
    else:
        if log_level:
            lv_value = getattr(logging, log_level.upper())
            logging.basicConfig(level=lv_value)
        else:
            logging.basicConfig(level=logging.INFO)

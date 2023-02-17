import os
import pdb

import yaml
from functools import reduce


def reduce_get(cfg, key):

    return reduce(lambda c, k: c[k], key.split("."), cfg)


class ConfigClass(object):
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def get(self, key):
        return reduce_get(self.cfg, key)


def read_config(config_name: str, root_path='../../../..') -> ConfigClass:

    yaml_file_name = "{}.yaml".format(config_name)
    opj = os.path.join
    cfg_root = opj(root_path, 'src/config/')
    path_yaml = opj(cfg_root, yaml_file_name)
    yaml_dict = yaml.load(open(path_yaml), Loader=yaml.FullLoader)

    return ConfigClass(yaml_dict)
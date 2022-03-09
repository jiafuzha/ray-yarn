import os
import yaml
import shutil
from typing import Dict, Any

yarn_configs = {}
head_configs = {}
worker_configs = {}

YARN_CONFIG_FILE_NAME = "yarn.yaml"
CONFIG_NAME_ROOT = "yarn"
CONFIG_NAME_HEAD = "head"
CONFIG_NAME_WORKER = "worker"
# find yarn.yaml from ~/.config/ray/, /etc/ray/ and package
# copy it to ~/.config/ray/ if it doesn't exist
USER_CONFIG_LOC = os.path.expanduser("~/.config/ray")
PATHS = [USER_CONFIG_LOC, "/etc/ray", os.path.dirname(os.path.realpath(__file__))]


class ConfigError(Exception):
    """Exception class for configuration error"""
    pass


def replace_hyphen(dct: Dict[str, Any]):
    if dct is None:
        return None
    new = {}
    for k, v in dct.items():
        if isinstance(v, dict):
            v = replace_hyphen(v)
        new[k.replace('-', '_')] = v
    return new


def load_config():
    global yarn_configs
    if yarn_configs:
        return

    for path in PATHS:
        yarn_file = path + "/" + YARN_CONFIG_FILE_NAME
        if os.path.isfile(yarn_file):
            break

    if not yarn_file.startswith(USER_CONFIG_LOC):
        os.makedirs(USER_CONFIG_LOC, exist_ok=True)
        shutil.copy(yarn_file, USER_CONFIG_LOC)
        yarn_file = USER_CONFIG_LOC + "/" + YARN_CONFIG_FILE_NAME

    with open(yarn_file) as f:
        data = f.read()

    root_configs = yaml.safe_load(data)
    if CONFIG_NAME_ROOT not in root_configs:
        raise ConfigError("Expect " + CONFIG_NAME_ROOT + " in first level in yarn.yaml")
    yarn_configs = root_configs[CONFIG_NAME_ROOT]
    yarn_configs = replace_hyphen(yarn_configs)
    if CONFIG_NAME_HEAD in yarn_configs and yarn_configs[CONFIG_NAME_HEAD] is not None:
        global head_configs
        head_configs = yarn_configs[CONFIG_NAME_HEAD]
    if CONFIG_NAME_WORKER in yarn_configs and yarn_configs[CONFIG_NAME_WORKER] is not None:
        global worker_configs
        worker_configs = yarn_configs[CONFIG_NAME_WORKER]

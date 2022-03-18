import os
import yaml
import shutil
from typing import Dict, Any
import re

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

MEMORY_SIZE_UNITS = {
    "B": 1,
    "K": 2 ** 10,
    "M": 2 ** 20,
    "G": 2 ** 30,
    "T": 2 ** 40,
    "P": 2 ** 50,
    "Ki": 2 ** 10,
    "Mi": 2 ** 20,
    "Gi": 2 ** 30,
    "Ti": 2 ** 40,
    "Pi": 2 ** 50,
}


class ConfigError(Exception):
    """Exception class for configuration error"""
    pass


def replace_hyphen_with_dash(dct: Dict[str, Any]):
    if dct is None:
        return None
    new = {}
    for k, v in dct.items():
        if isinstance(v, dict):
            v = replace_hyphen_with_dash(v)
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
    yarn_configs = replace_hyphen_with_dash(yarn_configs)
    if CONFIG_NAME_HEAD in yarn_configs and yarn_configs[CONFIG_NAME_HEAD] is not None:
        global head_configs
        head_configs = yarn_configs[CONFIG_NAME_HEAD]
    if CONFIG_NAME_WORKER in yarn_configs and yarn_configs[CONFIG_NAME_WORKER] is not None:
        global worker_configs
        worker_configs = yarn_configs[CONFIG_NAME_WORKER]


def parse_memory(resource):
    resource_str = str(resource)
    try:
        return int(resource_str)
    except ValueError:
        pass
    memory_size = re.sub(r"([BKMGTP]+[iB]*)", r" \1", resource_str, flags=re.IGNORECASE)
    number, unit_index = [item.strip() for item in memory_size.split()]
    unit = unit_index[0].upper()
    if len(unit_index) > 1:
        if unit_index[1] == 'i':
            unit += 'i'
        elif unit_index[1].upper() != 'B':
            raise ValueError("bad unit, " + resource + ". Allowed units are:\n" + ",".join(MEMORY_SIZE_UNITS.keys()))
    return float(number) * MEMORY_SIZE_UNITS[unit]

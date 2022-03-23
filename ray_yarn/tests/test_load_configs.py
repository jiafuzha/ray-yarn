import os
import yaml
from pathlib import Path
from ray_yarn import config


def test_yarn_config_loaded():
    config.load_config()
    assert config.yarn_configs
    assert config.yarn_configs["num_cpus"] == 1
    assert config.yarn_configs["memory"] == "2GiB"
    assert config.yarn_configs["specification"] is None


def test_load_bad_config(tmp_path):
    config.yarn_configs = {}
    current_yarn_file = Path(config.USER_CONFIG_LOC + "/" + config.YARN_CONFIG_FILE_NAME)
    has_current = True if current_yarn_file.is_file() else None
    if has_current:
        backup_file = tmp_path.joinpath(config.YARN_CONFIG_FILE_NAME)
        current_yarn_file.rename(backup_file)
    current_yarn_file.write_text("""
    worker:
      env: {'xyz': 1, 'abc': 2}
    """)
    ee: Exception = None
    try:
        config.load_config()
    except config.ConfigError as ce:
        ee = ce
    assert ee is not None
    # restore
    if has_current:
        backup_file.rename(current_yarn_file)
        config.load_config()
        assert config.yarn_configs


def test_load_yarn_yaml():
    yarn_file = os.path.dirname(os.path.realpath(__file__)) + "/../yarn.yaml"
    with open(yarn_file) as f:
        data = f.read()
    cfg = yaml.safe_load(data)
    yarn_cfg = cfg['yarn']
    assert yarn_cfg is not None
    assert yarn_cfg['num-cpus'] == 1
    head_cfg = yarn_cfg['head']
    assert head_cfg['port'] == 6379


def test_parse_env_literal():
    cfg = yaml.safe_load("""
    yarn:
      worker:
        env: {'xyz': 1, 'abc': 2}
    """)
    env = cfg['yarn']['worker']['env']
    assert env['xyz'] == 1
    assert env['abc'] == 2


def test_replace_hyphen():
    dct = {
        "yarn": {
            "num-cpus": 2,
            "object-store-memory": 200,
            "sub-head": {
                "num-cpus": 1
            }
        }
    }
    new_dct = config.replace_hyphen_with_dash(dct)
    assert "num_cpus" in new_dct["yarn"]
    assert "object_store_memory" in new_dct["yarn"]
    assert "num_cpus" in new_dct["yarn"]["sub_head"]


def test_parse_memory():
    size = "120"
    assert config.parse_memory(size) == 120
    size = "120B"
    assert config.parse_memory(size) == 120
    size = "120M"
    assert config.parse_memory(size) == 120 * config.MEMORY_SIZE_UNITS['M']
    size = "120m"
    assert config.parse_memory(size) == 120 * config.MEMORY_SIZE_UNITS['M']
    size = "120mb"
    assert config.parse_memory(size) == 120 * config.MEMORY_SIZE_UNITS['M']
    size = "120MB"
    assert config.parse_memory(size) == 120 * config.MEMORY_SIZE_UNITS['M']
    size = "1000KB"
    assert config.parse_memory(size) == 1000 * config.MEMORY_SIZE_UNITS['K']
    size = "1000b"
    assert config.parse_memory(size) == 1000 * config.MEMORY_SIZE_UNITS['B']
    size = "200kib"
    assert config.parse_memory(size) == 200 * config.MEMORY_SIZE_UNITS['Ki']
    size = "200Mib"
    assert config.parse_memory(size) == 200 * config.MEMORY_SIZE_UNITS['Mi']
    ee: Exception = None
    size = "200KIb"
    try:
        config.parse_memory(size)
    except ValueError as e:
        ee = e
    assert ee is not None
    msg = str(ee)
    assert size in msg
    assert ",".join(config.MEMORY_SIZE_UNITS.keys()) in msg

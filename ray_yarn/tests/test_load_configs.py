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
    new_dct = config.replace_hyphen(dct)
    assert "num_cpus" in new_dct["yarn"]
    assert "object_store_memory" in new_dct["yarn"]
    assert "num_cpus" in new_dct["yarn"]["sub_head"]

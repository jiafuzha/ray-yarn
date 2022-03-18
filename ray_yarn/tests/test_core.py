import pytest
from ray.ray_constants import DEFAULT_DASHBOARD_IP, DEFAULT_DASHBOARD_PORT
from ray_yarn import config, core


def test_bad_value_object_varargs():
    class A(core.Value):
        def __init__(self, a, b, *args, **kwargs):
            pass
    ee: Exception = None
    try:
        A(1, 2, 3, 4, 5, p1="p1", p2="p2")
    except ValueError as e:
        ee = e
    assert ee is not None
    assert "varargs is not supported" in str(ee)


def test_value_object_positional_args():
    class C(core.Value):
        def __init__(self, a, b, **kwargs):
            pass
    oc = C(1, "b")
    assert oc.a == 1
    assert oc.b == "b"
    assert "C(1,b)" in str(oc)
    oc2 = C(1, "b", c=4)
    assert oc2.a == 1
    assert oc2.b == "b"
    assert oc2.c == 4
    assert "C(1,b,4)" in str(oc2)
    assert oc != oc2
    oc3 = C(1, "b")
    assert oc == oc3


def test_value_object_values():
    class B(core.Value):
        def __init__(self, a=1, b=2, c="c"):
            pass
    ob = B()
    assert ob.a == 1
    assert ob.b == 2
    assert ob.c == "c"
    assert "B(1,2,c)" in str(ob)
    ob2 = B(a=10, c="cc")
    assert ob2.a == 10
    assert ob2.b == 2
    assert ob2.c == "cc"
    assert "B(10,2,cc)" in str(ob2)


def test_value_object_mix_keyword_values():
    class D(core.Value):
        def __init__(self, a=1, b=2, **kwargs):
            pass
    od = D()
    assert od.a == 1
    assert od.b == 2
    od2 = D(a=10, b=20)
    assert od2.a == 10
    assert od2.b == 20
    od3 = D(a=10, b=20, c="c", d="d")
    assert od3.a == 10
    assert od3.b == 20
    assert od3.c == "c"
    assert od3.d == "d"
    assert od != od2
    assert od != od3


def test_value_object_var_keyword_values():
    class E(core.Value):
        def __init__(self, **kwargs):
            pass
    oe = E(a="100", b="10", c=0.53)
    assert oe.a == "100"
    assert oe.b == "10"
    assert oe.c == 0.53
    assert hash(oe) == hash(str(oe))


@pytest.fixture
def load_config():
    config.load_config()


def _lookup_assert(kwargs, name, value1, value2, value3):
    assert core.lookup(kwargs, name, None) == value1
    assert core.lookup(kwargs, name, config.CONFIG_NAME_HEAD) == value2
    assert core.lookup(kwargs, name, config.CONFIG_NAME_WORKER) == value3


@pytest.mark.usefixtures("load_config")
def test_lookup():
    name = "num_cpus"
    kwargs = {name: 10}
    _lookup_assert(kwargs, name, 10, 10, 10)
    kwargs1 = {}
    _lookup_assert(kwargs1, name, 1, 1, 1)
    config.head_configs[name] = 20
    config.worker_configs[name] = 30
    _lookup_assert(kwargs1, name, 1, 20, 30)
    del config.head_configs[name]
    del config.worker_configs[name]
    ee: Exception = None
    try:
        core.lookup(kwargs1, name, "unknown")
    except KeyError as e:
        ee = e
    assert ee is not None
    assert "unknown prefix" in str(ee)


@pytest.mark.usefixtures("load_config")
def test_ray_runtime_cfg():
    cfg = core.RayRuntimeConfig()
    assert cfg.num_cpus is None
    assert len(cfg.__dict__) == 28
    cfg2 = core.RayRuntimeConfig(num_cpus=15, no_redirect_output=True)
    assert cfg2.num_cpus == 15
    assert cfg2.no_redirect_output


def cfg_assert_defaults(cfg):
    assert cfg.num_cpus == 1
    assert cfg.min_worker_port == core.RayRuntimeConfig.MIN_WORKER_PORT
    assert cfg.max_worker_port == core.RayRuntimeConfig.MAX_WORKER_PORT
    assert cfg.dashboard_host == DEFAULT_DASHBOARD_IP
    assert cfg.dashboard_port == DEFAULT_DASHBOARD_PORT


CUSTOM_MIN_PORT = 10004
CUSTOM_MAX_PORT = 20004
CUSTOM_DASH_HOST = "192.168.1.1"
CUSTOM_DASH_PORT = 8765
CUSTOM_NUM_CPUS = 13


def cfg_assert_customs(cfg):
    assert cfg.min_worker_port == CUSTOM_MIN_PORT
    assert cfg.max_worker_port == CUSTOM_MAX_PORT
    assert cfg.dashboard_host == CUSTOM_DASH_HOST
    assert cfg.dashboard_port == CUSTOM_DASH_PORT
    assert cfg.num_cpus == CUSTOM_NUM_CPUS


@pytest.mark.usefixtures("load_config")
def test_ray_to_head_cfg():
    cfg = core.RayRuntimeConfig()
    head_cfg = cfg.to_head_cfg()
    cfg_assert_defaults(head_cfg)
    cfg2 = core.RayRuntimeConfig(min_worker_port=CUSTOM_MIN_PORT, max_worker_port=CUSTOM_MAX_PORT,
                                 dashboard_host=CUSTOM_DASH_HOST, dashboard_port=CUSTOM_DASH_PORT)
    config.head_configs["num_cpus"] = CUSTOM_NUM_CPUS
    head_cfg2 = cfg2.to_head_cfg()
    cfg_assert_customs(head_cfg2)
    del config.head_configs["num_cpus"]


@pytest.mark.usefixtures("load_config")
def test_ray_to_worker_cfg():
    cfg = core.RayRuntimeConfig()
    worker_cfg = cfg.to_worker_cfg()
    cfg_assert_defaults(worker_cfg)
    cfg2 = core.RayRuntimeConfig(min_worker_port=CUSTOM_MIN_PORT, max_worker_port=CUSTOM_MAX_PORT,
                                 dashboard_host=CUSTOM_DASH_HOST, dashboard_port=CUSTOM_DASH_PORT)
    config.worker_configs["num_cpus"] = CUSTOM_NUM_CPUS
    worker_cfg2 = cfg2.to_worker_cfg()
    cfg_assert_customs(worker_cfg2)
    del config.worker_configs["num_cpus"]


@pytest.mark.usefixtures("load_config")
def test_yarn_cluster(skein_client):

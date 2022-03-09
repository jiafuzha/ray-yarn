import pytest

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



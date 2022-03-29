import pytest
import ray_yarn
from ray_yarn import cli, core


def test_extract_type():
    t = cli.extract_type("(typing.Union[int, NoneType], None)")
    assert t == "int"
    t = cli.extract_type("(typing.Union[str, NoneType], None)")
    assert t == "str"
    t = cli.extract_type("(typing.Union[bool, NoneType], None)")
    assert t == "bool"
    t = cli.extract_type("(typing.Union[typing.Dict[str, float], NoneType], None)")
    assert t == "typing.Dict[str, float]"
    t = cli.extract_type("unknown")
    assert t is None
    t = cli.extract_type("(<class 'int'>, None)")
    assert t == "int"
    t = cli.extract_type("(<class 'bool'>, None)")
    assert t == "bool"
    t = cli.extract_type("(<class 'str'>, None)")
    assert t == "str"
    t = cli.extract_type("(typing.Dict, None)")
    assert t == "typing.Dict"
    t = cli.extract_type("(typing.Dict[str, str], None)")
    assert t == "typing.Dict[str, str]"
    t = cli.extract_type("(<class 'ray_yarn.core.RayRuntimeConfig'>, None)")
    assert t == "ray_yarn.core.RayRuntimeConfig"


def test_extract_args_from_class():
    args = cli.extract_args_from_class(core.RayRuntimeConfig)
    for _, v in args.items():
        assert v[1] is not None
    assert args['num_cpus'][0] == 'int'
    assert args['num_gpus'][0] == 'int'
    assert args['resources'][0] == 'str'
    assert args['include_dashboard'][0] == 'bool'
    assert args['no_monitor'][0] == 'bool'
    assert len(args) == 28


def run_command(command, error=True):
    with pytest.raises(SystemExit) as exec:
        args = ["--" + arg for arg in command.split(" --") if arg]
        # remove -- at head
        args[0] = args[0][2:]
        cli.main(args)
    if error:
        assert exec.value != 0
    else:
        assert exec.value == 0


# def test_start(capfd):
#     run_command("start --head --port=6380 "
#                 "--min-worker-port=10005 "
#                 "--resources={\"name\": \"messenger\", \"playstore\": true, \"company\": \"Facebook\", \"price\": 100}")
#
#
# def test_start_failed(capfd):
#     run_command("start")


def test_stop(capfd):
    run_command("stop")


def test_status_missing_argument(capfd):
    run_command("status")
    out, err = capfd.readouterr()
    assert "the following arguments are required: APP_ID" in err


def test_help(capfd):
    run_command("-h")
    out, err = capfd.readouterr()
    assert "Start Ray Head or Worker" in out
    assert "Stop Ray Head or Worker" in out


def test_version(capfd):
    run_command("--version")
    out, err = capfd.readouterr()
    assert "ray-yarn " + ray_yarn.__version__ in out

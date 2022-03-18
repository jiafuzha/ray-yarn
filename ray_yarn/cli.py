from inspect import signature, Parameter
from collections import OrderedDict
import sys
import re
import argparse
import subprocess
import signal
import psutil
import time
from pydoc import locate
import skein
from skein.utils import humanize_timedelta, format_table
import ray_yarn
from ray_yarn import core
from core import _append_args, _get_skein_client

# search type from annotation in format "typing.Union[...]", "<class '...'>", or "typing...."
# E.g., typing.Union[str, NoneType], <class 'int'> and typing.Dict
_PATTERN_TYPE = re.compile(r"(\(?typing.Union\[(.+(?=, NoneType)).+\)?"
                           r"|\(?<class\s'(.+)'>.+\)?"
                           r"|\(?(typing.+(?=, None)).+\)?)")

_PATTERN_ARG_LINE = re.compile(r"([^:]+):.+")

_CLI_TYPES = ['str', 'int', 'bool']

_PARAMETER_LINE = "----------"

_RAY_STARTED_MSG = "Ray runtime started"

_PATTERN_RAY_CONNECT_INFO = r"ray\s+start\s+--address='([^']+)'\s+--redis-password='([^']+)'"

_RAY_HEAD_ADDRESS = "address"

_RAY_REDIS_PASSWORD = "redis-password"


def extract_type(annotation):
    m = _PATTERN_TYPE.match(annotation)
    if m:
        for i in range(2, len(m.groups()) + 1):
            if m.group(i):
                return m.group(i)
    return None


def add_help_doc(arg, args, line_trimmed, new_arg_expected):
    m = _PATTERN_ARG_LINE.match(line_trimmed)
    new_arg = arg
    if m:
        new_arg = m.group(1).strip()
    if new_arg_expected and arg == new_arg_expected:
        raise Exception("cannot extract argument from " + line_trimmed + ", wrong indent?")
    if new_arg_expected:
        if new_arg not in args:
            raise Exception("cannot find argument %s from constructor" % new_arg)
        else:
            return new_arg
    args[new_arg][1] = line_trimmed if args[new_arg][1] is None else args[new_arg][1] + "\n" + line_trimmed
    return new_arg


def extract_help(args, doc):
    started = False
    arg = None
    indent = ""
    for line in doc.splitlines():
        line_trimmed = line.strip()
        if started:
            if line_trimmed == _PARAMETER_LINE:
                break
            arg = add_help_doc(arg, args, line_trimmed, len(indent) == line.index(line_trimmed))
        else:
            started = line_trimmed == _PARAMETER_LINE
            if started:
                indent = line[0:line.index(line_trimmed)]


def extract_args_from_class(cls):
    sig = signature(cls.__init__)
    args = OrderedDict()

    for k, v in sig.parameters.items():
        if v.kind == Parameter.VAR_POSITIONAL:
            raise ValueError("varargs is not supported in __init__")
        elif v.kind != Parameter.VAR_KEYWORD and k != "self":
            t = extract_type(str(v.annotation))
            if t is None:
                raise TypeError("cannot extract any type from " + v.annotation)
            # use str for unknown types
            args[k] = [t if t in _CLI_TYPES else 'str', None]
    # get doc for argument help
    extract_help(args, cls.__doc__)
    return args


def convert_to_command_args(args):
    command_args = []
    for k, v in args.items():
        arg = k.replace('_', '-')
        command_args.append(("--" + arg, locate(v[0]), v[1]))
    return command_args


def add_help(parser):
    parser.add_argument(
        "--help", "-h", action="help", help="Show this help message then exit"
    )


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def arg(*args, **kwargs):
    return args, kwargs


command_runtime_args = convert_to_command_args(extract_args_from_class(core.RayRuntimeConfig))


def subcommand(subparser, name, help, command_runtime_args, *args):
    def decorate(func):
        parser = subparser.add_parser(
            name,
            help=help,
            description=help,
            add_help=False,
            argument_default=argparse.SUPPRESS
        )
        parser.set_defaults(func=func)
        add_help(parser)
        func.parser = parser
        for carg in command_runtime_args:
            parser.add_argument(carg[0], type=carg[1], help=carg[2])
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        return func

    return decorate


# setup root parser
yarn_parser = argparse.ArgumentParser(
    prog="ray-yarn",
    description="Deploy Ray on YARN",
    add_help=False,
    allow_abbrev=False,
    argument_default=argparse.SUPPRESS
)
yarn_parser.add_argument(
    "--version",
    action="version",
    version="%(prog)s " + ray_yarn.__version__,
    help="Show version then exit",
)
add_help(yarn_parser)
yarn_parser.set_defaults(func=lambda: fail(yarn_parser.format_usage()))
sub_parser = yarn_parser.add_subparsers(metavar="command", dest="command")
sub_parser.required = True


def _get_or_wait_kv(app_client, key, timeout):
    stime = 1
    value = app_client.kv.get(key)
    while value is None and timeout > 0:
        time.sleep(stime)
        value = app_client.kv.get(key)
        timeout -= stime
    if value is None:
        raise ValueError("cannot get key %s from kv store" % key)
    return value


def _format_args(is_head, app_client, **kwargs):
    args_line = []
    for k, v in kwargs.items():
        if v is not None:
            _append_args(k, v, args_line)
    if not is_head:
        if _RAY_HEAD_ADDRESS not in args_line:
            value = _get_or_wait_kv(app_client, _RAY_HEAD_ADDRESS, 30)
            _append_args(_RAY_HEAD_ADDRESS, value, args_line)
        if _RAY_REDIS_PASSWORD not in args_line:
            value = _get_or_wait_kv(app_client, _RAY_REDIS_PASSWORD, 30)
            _append_args(_RAY_REDIS_PASSWORD, value, args_line)
    return "".join(args_line)


def fail_ray_start_error(msg, proc):
    print(msg)
    print("exit code: " + proc.returncode)
    print(proc.stderr, sys.stderr)
    subprocess.run("ray stop", shell=True)


# sub-parser for ray start and stop
@subcommand(sub_parser, "start", "Start Ray Head or Worker", command_runtime_args,
            arg("--head", action='store_true', help="Provide this argument for the head node"),
            arg("--name", help="The application name"),
            arg("--queue", help="The queue to deploy to"),
            arg(
                "--user",
                help=(
                        "The user to submit the application on behalf of. Default "
                        "is the current user - submitting as a different user "
                        "requires proxy-user permissions."
                ),
            ),
            arg(
                "--tags",
                help=(
                        "A comma-separated list of strings to use as " "tags for this application."
                ),
            ),
            arg(
                "--environment",
                help=(
                        "Path to the Python environment to use. See the docs "
                        "for more information."
                ),
            ),
            )
def start(*args, **kwargs):
    app_client = skein.ApplicationClient.from_current()
    is_head = "head" in kwargs
    args_line = _format_args(is_head, app_client, **kwargs)
    print("ray start argument line: " + args_line)

    proc = subprocess.Popen(args_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True, bufsize=1, universal_newlines=True)
    pid = proc.pid
    print("ray process pid: " + pid)

    def kill(sig, frame):
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return
        children = parent.children(recursive=False)
        for process in children:
            print("terminating process: %s" % process)
            process.send_signal(sig)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, kill)

    # output and capture process info
    # ray instance started ? and connection info captured if it's head ?
    captured = [False, not is_head]

    def capture_process_info(msg):
        if (not captured[0]) and msg.index(_RAY_STARTED_MSG) > 0:
            captured[0] = True
        if not captured[1]:
            m = re.search(_PATTERN_RAY_CONNECT_INFO, msg)
            if m:
                captured[1] = True
                app_client.kv[_RAY_HEAD_ADDRESS] = m.group(1)
                app_client.kv[_RAY_REDIS_PASSWORD] = m.group(2)

    # wait for termination
    with proc:
        for line in proc.stdout:
            print(line, end='')
            capture_process_info(line)

    if proc.returncode != 0 and not all(captured):
        print("failed due to either ray processes not started or connection info"
              " (ray head process and redis password) not captured")
        kill(signal.SIGTERM, None)

    print(proc.stderr, file=sys.stderr)
    print("exit code: " + proc.returncode)


@subcommand(sub_parser, "stop", "Stop Ray Head or Worker", [],
            arg(
                "-f", "--force",
                help="If set, ray will send SIGKILL instead of SIGTERM."
            )
            )
def stop(*args, **kwargs):
    pass


app_id = arg("app_id", help="The application id", metavar="APP_ID")


@subcommand(
    sub_parser, "status", "Check the status of a submitted Ray application", [], app_id
)
def status(app_id):
    report = _get_skein_client().application_report(app_id)
    header = [
        "application_id",
        "name",
        "state",
        "status",
        "containers",
        "vcores",
        "memory",
        "runtime",
    ]
    data = [
        (
            report.id,
            report.name,
            report.state,
            report.final_status,
            report.usage.num_used_containers,
            report.usage.used_resources.vcores,
            report.usage.used_resources.memory,
            humanize_timedelta(report.runtime),
        )
    ]
    print(format_table(header, data))


@subcommand(sub_parser, "kill", "Kill a Ray application", app_id)
def kill(app_id):
    _get_skein_client().kill_application(app_id)


def main(args=None):
    kwargs = vars(yarn_parser.parse_args(args))
    kwargs.pop('command', None)
    func = kwargs.pop('func')
    func(**kwargs)
    sys.exit(0)


if __name__ == "__main__":
    main()

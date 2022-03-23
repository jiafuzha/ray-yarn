import weakref
from typing import Optional, Dict, List
from inspect import signature, Parameter
from copy import deepcopy
import time
import warnings
from urllib.parse import urlparse
import json
from . import config
from .config import CONFIG_NAME_HEAD, CONFIG_NAME_WORKER
import skein

_EXCLUDE_ARG_LIST = ["self", "num_cpus", "num_gpus", "memory", "initial_instances", "max_restarts"]
_EXCLUDE_ARG_LIST_WORKER = _EXCLUDE_ARG_LIST + ["gcs_server_port", "port", "include_dashboard", "dashboard_host",
                                                "dashboard_port"]
_IGNORE_ARG_VALUE_LIST = ["head", "block"]

_RAY_HEAD_ADDRESS = "address"

_RAY_REDIS_PASSWORD = "redis_password"


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


def _append_args(key, value, args_line):
    arg = ["--", key.replace('_', '-')]
    if key in _IGNORE_ARG_VALUE_LIST:
        args_line.append("".join(arg))
        return
    arg.append('=')
    if isinstance(value, str):
        value = value if value.find(' ') < 0 else "'" + value + "'"
        arg.append(value)
    elif isinstance(value, dict):
        value = json.dumps(value)
        arg.append("'" + value + "'")
    else:
        arg.append(str(value))
    args_line.append("".join(arg))


def _get_skein_client(skein_client=None, security=None):
    if skein_client is None:
        # Silence warning about credentials not being written yet
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return skein.Client(security=security)
    return skein_client


def lookup_yarn_config(name, prefix):
    value = config.yarn_configs.get(name)
    if prefix is None:
        return value
    elif prefix == CONFIG_NAME_HEAD:
        return value if name not in config.head_configs else config.head_configs[name]
    elif prefix == CONFIG_NAME_WORKER:
        return value if name not in config.worker_configs else config.worker_configs[name]
    else:
        raise KeyError("unknown prefix, " + prefix)


def lookup(kwargs, name, prefix):
    return kwargs[name] if kwargs.get(name) is not None else lookup_yarn_config(name, prefix)


def _files_and_build_script(environment):
    parsed = urlparse(environment)
    scheme = parsed.scheme

    if scheme in {"conda", "venv", "python"}:
        path = environment[len(scheme) + 3:]
        files = {}
        if scheme == "conda":
            setup = "conda activate %s" % path
            cli = "ray-yarn"
        elif scheme == "venv":
            setup = "source %s/bin/activate" % path
            cli = "ray-yarn"
        else:
            setup = ""
            cli = "%s -m ray_yarn.cli" % path
    else:
        files = {"environment": environment}
        setup = "source environment/bin/activate"
        cli = "environment/bin/python -m ray_yarn.cli"

    def build_script(cmd):
        command = "%s %s" % (cli, cmd)
        return "\n".join([setup, command]) if setup else command

    return files, build_script


def _construct_args(runtime_cfg, head):
    excluded = _EXCLUDE_ARG_LIST if head else _EXCLUDE_ARG_LIST_WORKER
    args_list = []
    for k, v in vars(runtime_cfg).items():
        if (k not in excluded) and v is not None:
            _append_args(k, v, args_list)
    return args_list


def _make_specification(**kwargs):
    """Create specification to run Ray Cluster

    This creates a ``skein.ApplicationSpec`` to run a ray cluster with
    ray head and ray workers. See the docstring for ``YarnCluster`` for
    more details.
    """
    spec = config.yarn_configs.get("specification")
    if spec:
        if isinstance(spec, dict):
            return skein.ApplicationSpec.from_dict(spec)
        return skein.ApplicationSpec.from_file(spec)

    name = lookup(kwargs, "name", None)
    queue = lookup(kwargs, "queue", None)
    tags = lookup(kwargs, "tags", None)
    user = lookup(kwargs, "user", None)
    environment = lookup(kwargs, "environment", None)
    # TODO URL for environment
    if environment is None:
        msg = (
            "You must provide a path to a Python environment for the ray head and worker.\n"
            "This may be one of the following:\n"
            "- A conda environment archived with conda-pack\n"
            "- A virtual environment archived with venv-pack\n"
            "- A path to a conda environment, specified as conda://...\n"
            "- A path to a virtual environment, specified as venv://...\n"
            "- A path to a python binary to use, specified as python://...\n"
            "\n"
            "See URL?? for more information."
        )
        raise ValueError(msg)

    files, build_script = _files_and_build_script(environment)

    cfg = kwargs['ray_runtime_cfg']
    head_cfg = cfg.to_head_cfg()
    services = {"ray.head": skein.Service(
        instances=1,
        resources=skein.Resources(
            vcores=head_cfg.num_cpus, memory=head_cfg.memory, gpus=head_cfg.num_gpus
        ),
        max_restarts=0,
        files=files,
        script=build_script("start --head --block " + " ".join(_construct_args(head_cfg, True))),
    )}
    worker_cfg = cfg.to_worker_cfg()
    services["ray.worker"] = skein.Service(
        instances=worker_cfg.initial_instances,
        resources=skein.Resources(
            vcores=worker_cfg.num_cpus, memory=worker_cfg.memory, gpus=worker_cfg.num_gpus
        ),
        max_restarts=worker_cfg.max_restarts,
        depends=["ray.head"],
        files=files,
        script=build_script("start --block " + " ".join(_construct_args(worker_cfg, False)))
    )
    spec = skein.ApplicationSpec(
        name=name, queue=queue, tags=tags, user=user, services=services
    )

    return spec


class Value(object):
    @staticmethod
    def _get_value(key, value, index, arg_list, kwargs):
        if index < len(arg_list):
            return arg_list[index]
        elif key in kwargs:
            return kwargs[key]
        else:
            return value.default if value.default != Parameter.empty else None

    def __new__(cls, *args, **kwargs):
        self = object.__new__(cls)
        sig = signature(self.__init__)
        arg_list = list(args)
        index = 0
        for k, v in sig.parameters.items():
            if v.kind == Parameter.VAR_POSITIONAL:
                raise ValueError("varargs is not supported in __init__")
            elif v.kind != Parameter.VAR_KEYWORD:
                self.__dict__[k] = Value._get_value(k, v, index, arg_list, kwargs)
            index += 1
        self.__dict__.update(kwargs)

        return self

    def __repr__(self):
        values = [str(v) for _, v in self.__dict__.items()]
        return "%s(%s)" % (self.__class__.__name__, ",".join(values))

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __neg__(self, other):
        return repr(self) != repr(other)


# TODO replace URL with real one
class RayRuntimeConfig(Value):
    """Ray head and worker configurations.

    You can define default values for this in the ``yarn.yaml``
    configuration file. See ##URL for more information.
    
    Parameters (from which command line arguments in cli.py extract)
    ----------
    num_cpus: Optional[int] = None
        Number of CPUs of node.
    num_gpus: Optional[int] = None
        Number of GPUs of node.
    resources: Dict[str, float] = None
        Customized resources. In command line, use JSON serialized dictionary mapping
        resource name to resource quantity.
    memory: Optional[int] = None
        Amount of memory.
    port: Optional[int] = None
        The port of the head ray process. If not provided, defaults to 6379; if port is set
        to 0, we will allocate an available port.
    initial_instances: Optional[int] = None
        Number of workers to start on initialization.
    object_store_memory: Optional[int] = None
        Amount of memory to start the object store with. By default, this is automatically set
        based on available system memory.
    plasma_directory: Optional[str] = None
        Object store directory for memory mapped files.
    include_dashboard: Optional[bool] = None
        Boolean flag to start ray dashboard GUI. By default, the dashboard is started.
    dashboard_host: Optional[str] = None
        The host to bind the dashboard server to, either localhost(127.0.0.1) or 0.0.0.0. By
        default, this is localhost.
    dashboard_port: Optional[int] = None
        The port to bind the dashboard server to. Defaults to 8265.
    object_manager_port: Optional[int] = None
        The port to use for starting the object manager.
    node_manager_port: Optional[int] = None
        The port to use for starting the node manager.
    gcs_server_port: Optional[int] = None
        Port for the server.
    min_worker_port: Optional[int] = None
        The lowest port number that workers will bind on.
    max_worker_port: Optional[int] = None
        The highest port number that workers will bind on.
    worker_port_list: Optional[str] = None
        A comma-separated list of open ports for workers to bind on. Overrides 'min-worker-port'
        and 'max-worker-port'.
    max_restarts: Optional[int] = None
        Allowed number of worker restarts, -1 for unlimited.
    autoscaling_config: Optional[str] = None
        The file that contains the autoscaling config.
    no_redirect_output: Optional[bool] = None
        Do not redirect non-worker stdout and stderr to files.
    plasma_store_socket_name: Optional[str] = None
        Socket name of the plasma store.
    raylet_socket_name: Optional[str] = None
        Socket name of raylet process.
    enable_object_reconstruction: Optional[bool] = None
        Reconstruction object when it's lost.
    temp_dir: Optional[str] = None
        Root temporary directory for the Ray process.
    no_monitor: Optional[bool] = None
        If True, the ray autoscaler monitor for this cluster will not be started.
    redis_password: Optional[str] = None
        Redis password.
    redis_max_memory: Optional[int] = None.
        redis max memory
    redis_shard_ports: Optional[str] = None
        redis shard ports
    ----------
    """

    MIN_WORKER_PORT = 10000
    MAX_WORKER_PORT = 10999

    def __init__(
        self,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[int] = None,
        resources: Optional[Dict[str, float]] = None,
        memory: Optional[int] = None,
        port: Optional[int] = None,
        initial_instances: Optional[int] = 0,
        object_store_memory: Optional[int] = None,
        plasma_directory: Optional[str] = None,
        include_dashboard: Optional[bool] = None,
        dashboard_host: Optional[str] = None,
        dashboard_port: Optional[int] = None,
        object_manager_port: Optional[int] = None,
        node_manager_port: Optional[int] = None,
        gcs_server_port: Optional[int] = None,
        min_worker_port: Optional[int] = None,
        max_worker_port: Optional[int] = None,
        worker_port_list: Optional[str] = None,
        max_restarts: Optional[int] = None,
        autoscaling_config: Optional[str] = None,
        no_redirect_output: Optional[bool] = None,
        plasma_store_socket_name: Optional[str] = None,
        raylet_socket_name: Optional[str] = None,
        enable_object_reconstruction: Optional[bool] = None,
        temp_dir: Optional[str] = None,
        no_monitor: Optional[bool] = None,
        redis_password: Optional[str] = None,
        redis_max_memory: Optional[int] = None,
        redis_shard_ports: Optional[str] = None
    ):
        pass

    def _fallback_values(self, new_cfg, prefix):
        for k, v in self.__dict__.items():
            if v is None:
                new_cfg.__setattr__(k, lookup_yarn_config(k, prefix))

    @staticmethod
    def _set_default_values(new_cfg):
        pass
        # if new_cfg.dashboard_host is None:
        #     new_cfg.dashboard_host = DEFAULT_DASHBOARD_IP
        # if new_cfg.dashboard_port is None:
        #     new_cfg.dashboard_port = DEFAULT_DASHBOARD_PORT
        # if new_cfg.min_worker_port is None:
        #     new_cfg.min_worker_port = RayRuntimeConfig.MIN_WORKER_PORT
        # if new_cfg.max_worker_port is None:
        #     new_cfg.max_worker_port = RayRuntimeConfig.MAX_WORKER_PORT

    def to_head_cfg(self):
        head_cfg = deepcopy(self)
        self._fallback_values(head_cfg, config.CONFIG_NAME_HEAD)
        RayRuntimeConfig._set_default_values(head_cfg)
        return head_cfg

    def to_worker_cfg(self):
        worker_cfg = deepcopy(self)
        self._fallback_values(worker_cfg, config.CONFIG_NAME_WORKER)
        RayRuntimeConfig._set_default_values(worker_cfg)
        return worker_cfg


class RayYarnError(skein.SkeinError):
    """An error involving the ray-yarn skein Application"""

    pass


def submit_and_handle_failures(skein_client, spec):
    app_id = skein_client.submit(spec)
    try:
        try:
            return skein_client.connect(app_id, security=spec.master.security)
        except BaseException:
            # Kill the application on any failures
            skein_client.kill_application(app_id)
            raise
    except (skein.ConnectionError, skein.ApplicationNotRunningError):
        # If the error was an application error, raise appropriately
        raise RayYarnError(
            (
                "Failed to start ray-yarn {app_id}\n"
                "See the application logs for more information:\n\n"
                "$ yarn logs -applicationId {app_id}"
            ).format(app_id=app_id)
        )


class YarnCluster(object):

    """Start a Ray cluster on YARN.

    You can define default values for this in the ``yarn.yaml``
    configuration file. See ##URL for more information.

    Parameters (from which command line arguments in cli.py extract)
    ----------
    ray_runtime_cfg: RayRuntimeConfig = RayRuntimeConfig()
        Ray runtime configuration. See doc in RayRuntimeConfig
    environment: Optional[str] = None
        The Python environment to use. Can be one of the following:

          - A path to an archived Python environment
          - A path to a conda environment, specified as `conda:///...`
          - A path to a virtual environment, specified as `venv:///...`
          - A path to a python executable, specifed as `python:///...`

        Note that if not an archive, the paths specified must be valid on all
        nodes in the cluster.
    name: Optional[str] = None
        The application name.
    queue: Optional[str] = None
        The queue to deploy to.
    tags: List[str] = None
        A set of strings to use as tags for this application.
    user: Optional[str] = None
        The user to submit the application on behalf of. Default is the current
        user - submitting as a different user requires user permissions, see
        the YARN documentation for more information.
    skein_client: Optional[skein.Client] = None
        The ``skein.Client`` to use. If not provided, one will be started.
    ----------
    """
    def __init__(
        self,
        ray_runtime_cfg: RayRuntimeConfig = RayRuntimeConfig(),
        environment: Optional[str] = None,
        name: Optional[str] = None,
        queue: Optional[str] = None,
        tags: List[str] = None,
        user: Optional[str] = None,
        skein_client: Optional[skein.Client] = None
    ):
        self.spec = _make_specification(
            ray_runtime_cfg=ray_runtime_cfg,
            environment=environment,
            name=name,
            queue=queue,
            tags=tags,
            user=user
        )
        self._requested = set()
        self._skein_client = skein_client
        self._start_cluster()
        self._home_ip = None
        self._redis_password = None
        self._finalizer = weakref.finalize(self, self.application_client.shutdown)

    @property
    def app_id(self):
        return self.application_client.id

    def get_home_ip(self, timeout=30):
        if self._home_ip is not None:
            return self._home_ip
        value = _get_or_wait_kv(self.application_client, _RAY_HEAD_ADDRESS, timeout)
        self._home_ip = value.decode().split(':')[0]
        return self._home_ip

    def get_redis_password(self, timeout=30):
        if self._redis_password is not None:
            return self._redis_password
        value = _get_or_wait_kv(self.application_client, _RAY_REDIS_PASSWORD, timeout)
        self._redis_password = value.decode()
        return self._redis_password

    def _start_cluster(self):
        """Start the cluster and initialize state"""
        skein_client = _get_skein_client(self._skein_client)
        self.application_client = submit_and_handle_failures(skein_client, self.spec)

    def _scale_up(self, n):
        if n > len(self._requested):
            containers = self.application_client.scale("ray.worker", n)
            self._requested.update(c.id for c in containers)

    def _scale(self, n):
        if n >= len(self._requested):
            return self._scale_up(n)

    def scale(self, n):
        """Scale cluster to n workers.

        Parameters
        ----------
        n : int
            Target number of workers

        Examples
        --------
        >>> cluster.scale(10)  # scale cluster to ten workers
        """
        return self._scale(n)

    def workers(self):
        """A list of all currently running worker containers."""
        return self._workers()

    def _workers(self):
        return self.application_client.get_containers(services=["ray.worker"])

    def shutdown(self, status="SUCCEEDED", diagnostics=None):
        """Shutdown the application.

        Parameters
        ----------
        status : {'SUCCEEDED', 'FAILED', 'KILLED'}, optional
            The yarn application exit status.
        diagnostics : str, optional
            The application exit message, usually used for diagnosing failures.
            Can be seen in the YARN Web UI for completed applications under
            "diagnostics". If not provided, a default will be used.
        """
        if self._finalizer is not None and self._finalizer.peek() is not None:
            self.application_client.shutdown(status=status, diagnostics=diagnostics)
            self._finalizer.detach()  # don't run the finalizer later
        self._finalizer = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    def __repr__(self):
        return "YarnCluster<%s>" % self.app_id

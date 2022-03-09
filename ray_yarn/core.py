from typing import Optional, Dict, List
from inspect import signature, Parameter
from ray.ray_constants import DEFAULT_DASHBOARD_IP, DEFAULT_DASHBOARD_PORT
from . import config
from .config import CONFIG_NAME_HEAD, CONFIG_NAME_WORKER
import skein


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




    return None


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


class RayRuntimeConfig(Value):
    def __init__(
        self,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[int] = None,
        resources: Dict[str, float] = {},
        memory: Optional[int] = None,
        head_port: Optional[int] = None,
        n_workers: Optional[int] = None,
        object_store_memory: Optional[int] = None,
        plasma_directory: Optional[str] = None,
        include_dashboard: Optional[bool] = None,
        dashboard_host: str = DEFAULT_DASHBOARD_IP,
        dashboard_port: int = DEFAULT_DASHBOARD_PORT,
        object_manager_port: Optional[int] = None,
        node_manager_port: Optional[int] = None,
        gcs_server_port: Optional[int] = None,
        min_worker_port: Optional[int] = 10000,
        max_worker_port: Optional[int] = 10999,
        worker_port_list: Optional[str] = None,
        worker_restarts: Optional[int] = None,
        autoscaling_config: Optional[str] = None,
        no_redirect_output: Optional[bool] = None,
        plasma_store_socket_name: Optional[str] = None,
        raylet_socket_name: Optional[str] = None,
        temp_dir: Optional[str] = None,
        enable_object_reconstruction: Optional[bool] = None,
        no_monitor: Optional[bool] = None,
        redis_password: Optional[str] = None,
        redis_max_memory: Optional[int] = None,
        redis_shard_ports: Optional[str] = None
    ):
        pass


class YarnCluster(object):
    def __init__(
        self,
        ray_runtime_cfg: RayRuntimeConfig = RayRuntimeConfig(),
        environment: Optional[str] = None,
        name: Optional[str] = None,
        queue: Optional[str] = None,
        tags: List[str] = None,
        user: Optional[str] = None,
        skein_client: skein.Client = None
    ):
        spec = _make_specification(
            ray_runtime_cfg=ray_runtime_cfg,
            environment=environment,
            name=name,
            queue=queue,
            tags=tags,
            user=user
        )



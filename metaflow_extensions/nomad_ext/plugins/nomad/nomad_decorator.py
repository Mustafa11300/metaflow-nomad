import os
import sys

# IMPORTANT: Do NOT import anything from metaflow at the module top level.
# This module is loaded by Metaflow's plugin resolution system during
# metaflow.__init__. Any import of metaflow here creates a circular import:
#   metaflow.__init__ -> plugins.__init__ -> resolve_plugins() -> get_plugin()
#   -> importlib.import_module(this_module) -> from metaflow.X -> metaflow.__init__
# Since metaflow.__init__ is not yet complete, the re-entrant import returns
# a partially-loaded module, and our class isn't found.
#
# All metaflow imports are deferred to method bodies, which are only called
# AFTER metaflow is fully initialized.


class NomadDecorator(object):
    """
    Specifies that this step should execute on a HashiCorp Nomad cluster.

    Parameters
    ----------
    address : str, optional, default None
        Address of the Nomad API (e.g. http://127.0.0.1:4646).
        If not specified, defaults to METAFLOW_NOMAD_ADDRESS environment variable.
    token : str, optional, default None
        ACL token for Nomad authentication. If not specified,
        defaults to METAFLOW_NOMAD_TOKEN environment variable.
    region : str, optional, default None
        Nomad region to submit the job to.
    namespace : str, optional, default None
        Nomad namespace to submit the job to.
    docker_image : str, optional, default None
        Docker image for the task. Defaults to python:3.11-slim.
    cpu : int, optional, default 500
        CPU resources in MHz to allocate for the task.
    memory : int, optional, default 256
        Memory in MB to allocate for the task.
    """

    name = "nomad"
    # IS_STEP_DECORATOR is checked by some parts of metaflow
    IS_STEP_DECORATOR = True

    defaults = {
        "address": None,
        "token": None,
        "region": None,
        "namespace": None,
        "docker_image": None,
        "cpu": 500,
        "memory": 256,
        "driver": None,
        "datacenters": None,
    }

    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        # Defer import: safe here because __init__ is only called after
        # metaflow is fully loaded
        from metaflow.decorators import StepDecorator

        # Fixup the class hierarchy at first instantiation
        if NomadDecorator.__bases__ == (object,):
            NomadDecorator.__bases__ = (StepDecorator,)

        super(NomadDecorator, self).__init__(attributes, statically_defined)

        from metaflow.metaflow_config import (
            NOMAD_ADDRESS,
            NOMAD_TOKEN,
            NOMAD_REGION,
            NOMAD_NAMESPACE,
            NOMAD_DOCKER_IMAGE,
            NOMAD_DRIVER,
            NOMAD_DATACENTERS,
        )

        if not self.attributes["address"]:
            self.attributes["address"] = NOMAD_ADDRESS
        if not self.attributes["token"]:
            self.attributes["token"] = NOMAD_TOKEN
        if not self.attributes["region"]:
            self.attributes["region"] = NOMAD_REGION
        if not self.attributes["namespace"]:
            self.attributes["namespace"] = NOMAD_NAMESPACE
        if not self.attributes["docker_image"]:
            self.attributes["docker_image"] = NOMAD_DOCKER_IMAGE
        if not self.attributes["driver"]:
            self.attributes["driver"] = NOMAD_DRIVER
        if not self.attributes["datacenters"]:
            self.attributes["datacenters"] = NOMAD_DATACENTERS

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        from metaflow.exception import MetaflowException

        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        if any(deco.name == "parallel" for deco in decos):
            raise MetaflowException(
                "Step *{step}* contains a @parallel decorator "
                "with the @nomad decorator. @parallel is not supported "
                "with @nomad.".format(step=step)
            )

    def package_init(self, flow, step_name, environment):
        from .nomad_exceptions import NomadException

        try:
            import nomad as _nomad_lib  # noqa: F401
        except (NameError, ImportError, ModuleNotFoundError):
            raise NomadException(
                "Could not import module 'python-nomad'.\n\nInstall python-nomad "
                "Python package (https://pypi.org/project/python-nomad/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install python-nomad\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

    def runtime_init(self, flow, graph, package, run_id):
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["nomad", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options["run-time-limit"] = self.run_time_limit
            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        from metaflow.metadata_provider import MetaDatum

        self.metadata = metadata
        self.task_datastore = task_datastore

        meta = {}
        if "METAFLOW_NOMAD_WORKLOAD" in os.environ:
            meta["nomad-alloc-id"] = os.environ.get("NOMAD_ALLOC_ID")
            meta["nomad-alloc-name"] = os.environ.get("NOMAD_ALLOC_NAME")
            meta["nomad-job-name"] = os.environ.get("NOMAD_JOB_NAME")
            meta["nomad-region"] = os.environ.get("NOMAD_REGION")
            meta["nomad-dc"] = os.environ.get("NOMAD_DC")
            meta["nomad-namespace"] = os.environ.get("NOMAD_NAMESPACE")
            meta["nomad-node-id"] = os.environ.get("NOMAD_NODE_ID")

        if len(meta) > 0:
            entries = [
                MetaDatum(
                    field=k,
                    value=v,
                    type=k,
                    tags=["attempt_id:{0}".format(retry_count)],
                )
                for k, v in meta.items()
                if v is not None
            ]
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        if "METAFLOW_NOMAD_WORKLOAD" in os.environ:
            from metaflow.metadata_provider.util import sync_local_metadata_to_datastore
            from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

            if hasattr(self, "metadata") and self.metadata.TYPE == "local":
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]

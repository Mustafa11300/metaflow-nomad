import os
import shlex
import atexit
import time
import sys

from metaflow import util

from metaflow.metaflow_config import (
    SERVICE_INTERNAL_URL,
    SERVICE_HEADERS,
    DEFAULT_METADATA,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
)

from metaflow.metaflow_config_funcs import config_values

from metaflow.mflog import (
    export_mflog_env_vars,
    bash_capture_logs,
    BASH_SAVE_LOGS,
)

from .nomad_client import NomadClient
from .nomad_exceptions import NomadException, NomadKilledException
from .nomad_job import NomadJob


# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class Nomad(object):
    """
    Main orchestration class for running Metaflow steps on Nomad.
    Mirrors the Slurm class from the @slurm extension.
    """

    def __init__(
        self,
        datastore,
        metadata,
        environment,
        nomad_access_params,
    ):
        self.datastore = datastore
        self.metadata = metadata
        self.environment = environment

        # Extract Nomad-specific params
        self.docker_image = nomad_access_params.pop("docker_image", "python:3.11-slim")
        self.cpu = nomad_access_params.pop("cpu", 500)
        self.memory = nomad_access_params.pop("memory", 256)
        self.driver = nomad_access_params.pop("driver", "docker")
        self.datacenters = nomad_access_params.pop("datacenters", None)

        # Create client with remaining params (address, token, region, namespace)
        client_params = {
            k: v
            for k, v in nomad_access_params.items()
            if k in ("address", "token", "region", "namespace") and v is not None
        }
        self.nomad_client = NomadClient(**client_params)

        atexit.register(lambda: self.job.kill() if hasattr(self, "job") else None)

    def _job_name(self, user, flow_name, run_id, step_name, task_id, retry_count):
        return "{user}-{flow_name}-{run_id}-{step_name}-{task_id}-{retry_count}".format(
            user=user,
            flow_name=flow_name,
            run_id=str(run_id) if run_id is not None else "",
            step_name=step_name,
            task_id=str(task_id) if task_id is not None else "",
            retry_count=str(retry_count) if retry_count is not None else "",
        )

    def _command(self, environment, code_package_url, step_name, step_cmds, task_spec):
        mflog_expr = export_mflog_env_vars(
            datastore_type=self.datastore.TYPE,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
            **task_spec
        )
        init_cmds = environment.get_package_commands(
            code_package_url, self.datastore.TYPE
        )
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                environment.bootstrap_commands(step_name, self.datastore.TYPE)
                + step_cmds
            )
        )

        # Construct an entry point that:
        # 1) initializes the mflog environment (mflog_expr)
        # 2) bootstraps a metaflow environment (init_expr)
        # 3) executes a task (step_expr)
        cmd_str = "true && mkdir -p %s && %s && %s && %s; " % (
            LOGS_DIR,
            mflog_expr,
            init_expr,
            step_expr,
        )
        # After the task has finished, save its exit code and persist final logs.
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        # Support sandbox init scripts
        cmd_str = (
            '${METAFLOW_INIT_SCRIPT:+eval \\\\"${METAFLOW_INIT_SCRIPT}\\\\"} && %s'
            % cmd_str
        )

        return shlex.split('bash -c "%s"' % cmd_str)

    def create_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        docker_image=None,
        cpu=None,
        memory=None,
        run_time_limit=None,
        env=None,
        attrs=None,
        driver=None,
        datacenters=None,
    ) -> NomadJob:
        if env is None:
            env = {}
        if attrs is None:
            attrs = {}

        job_name = self._job_name(
            attrs.get("metaflow.user"),
            attrs.get("metaflow.flow_name"),
            attrs.get("metaflow.run_id"),
            attrs.get("metaflow.step_name"),
            attrs.get("metaflow.task_id"),
            attrs.get("metaflow.retry_count"),
        )

        command = self._command(
            self.environment, code_package_url, step_name, [step_cli], task_spec
        )

        # Build environment variables for the Nomad task
        task_env = dict(env)
        # Propagate Metaflow configuration to the remote task
        for key, val in config_values():
            if val:
                task_env["METAFLOW_%s" % key] = str(val)

        # Set metadata and datastore config
        if SERVICE_INTERNAL_URL:
            task_env["METAFLOW_SERVICE_URL"] = SERVICE_INTERNAL_URL
        if SERVICE_HEADERS:
            task_env["METAFLOW_SERVICE_HEADERS"] = SERVICE_HEADERS
        if DEFAULT_METADATA:
            task_env["METAFLOW_DEFAULT_METADATA"] = DEFAULT_METADATA
        if DATASTORE_SYSROOT_S3:
            task_env["METAFLOW_DATASTORE_SYSROOT_S3"] = DATASTORE_SYSROOT_S3
        if DATATOOLS_S3ROOT:
            task_env["METAFLOW_DATATOOLS_S3ROOT"] = DATATOOLS_S3ROOT

        # Mark this as a Nomad workload for the decorator's task_pre_step
        task_env["METAFLOW_NOMAD_WORKLOAD"] = "1"

        self.job = NomadJob(
            client=self.nomad_client,
            name=job_name,
            command=command,
            docker_image=docker_image or self.docker_image,
            cpu=cpu or self.cpu,
            memory=memory or self.memory,
            env=task_env,
            region=self.nomad_client.client.__dict__.get("region"),
            namespace=self.nomad_client.namespace or "default",
            driver=driver or self.driver,
            datacenters=datacenters or self.datacenters,
        )

        return self.job

    def run_job(self, job: NomadJob, timeout: int = 3600, echo_logs: bool = True):
        """
        Submit a job, wait for it to complete, and return the exit code.

        Parameters
        ----------
        job : NomadJob
            The job to run.
        timeout : int
            Maximum time to wait for completion.
        echo_logs : bool
            If True, print logs to stdout as they come in.

        Returns
        -------
        int
            The exit code of the job.
        """
        print("    Submitting Nomad job '%s'..." % job.name)
        job.submit()

        print("    Waiting for allocation...")
        alloc = job.wait_for_running(timeout=min(timeout, 300))
        alloc_id_short = job.alloc_id[:8] if job.alloc_id else "unknown"
        print("    Allocation %s is running." % alloc_id_short)

        print("    Waiting for job to complete...")
        final_alloc = job.wait_for_completion(timeout=timeout)

        exit_code = job.get_exit_code()
        client_status = final_alloc.get("ClientStatus", "unknown")
        print(
            "    Job completed with status '%s' (exit code: %s)"
            % (client_status, exit_code)
        )

        if echo_logs:
            stdout = job.get_logs("stdout")
            stderr = job.get_logs("stderr")
            if stdout:
                print("\n--- Nomad task stdout ---")
                print(stdout)
            if stderr:
                print("\n--- Nomad task stderr ---", file=sys.stderr)
                print(stderr, file=sys.stderr)

        return exit_code

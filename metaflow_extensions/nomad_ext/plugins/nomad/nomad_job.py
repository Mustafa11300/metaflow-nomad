import re
import atexit
from typing import Optional, Dict, Any, List

from .nomad_client import NomadClient
from .nomad_exceptions import NomadException, NomadKilledException


def _sanitize_name(name: str) -> str:
    """Keep only alphanumeric characters, hyphens, and underscores."""
    return re.sub(r"[^a-zA-Z0-9_\-]", "-", name)[:128]


class NomadJob(object):
    """
    Represents a Nomad batch job for executing a Metaflow step.
    Generates the Nomad job specification and manages the job lifecycle.
    """

    def __init__(
        self,
        client: NomadClient,
        name: str,
        command: List[str],
        docker_image: str = "python:3.11-slim",
        cpu: int = 500,
        memory: int = 256,
        env: Optional[Dict[str, str]] = None,
        region: Optional[str] = None,
        namespace: str = "default",
        driver: str = "docker",
        datacenters: Optional[List[str]] = None,
    ):
        self.client = client
        self.name = _sanitize_name(name)
        self.command = command
        self.docker_image = docker_image
        self.cpu = cpu
        self.memory = memory
        self.env = env or {}
        self.region = region
        self.namespace = namespace
        self.driver = driver
        self.datacenters = datacenters or ["dc1"]

        self._job_id = None
        self._eval_id = None
        self._alloc_id = None

        # Register kill on exit to clean up if the process is interrupted
        atexit.register(self.kill)

    @property
    def job_id(self) -> Optional[str]:
        return self._job_id

    @property
    def alloc_id(self) -> Optional[str]:
        return self._alloc_id

    def _build_task_config(self, cmd: str, args: List[str]) -> Dict[str, Any]:
        """
        Build the task Config block based on the selected driver.
        """
        if self.driver == "raw_exec":
            return {
                "command": cmd,
                "args": args,
            }
        else:
            # Default: docker driver
            return {
                "image": self.docker_image,
                "command": cmd,
                "args": args,
            }

    def _build_job_spec(self) -> Dict[str, Any]:
        """
        Build a Nomad job specification dict for a batch job.
        Supports both 'docker' and 'raw_exec' task drivers.
        """
        # Build the command as a shell invocation
        # The first element is the entrypoint, rest are args
        if self.command:
            cmd = self.command[0]
            args = self.command[1:] if len(self.command) > 1 else []
        else:
            cmd = "/bin/bash"
            args = ["-c", "echo 'No command specified'"]

        task_config = self._build_task_config(cmd, args)

        job_spec = {
            "ID": self.name,
            "Name": self.name,
            "Type": "batch",
            "Datacenters": self.datacenters,
            "TaskGroups": [
                {
                    "Name": "metaflow-group",
                    "Count": 1,
                    "RestartPolicy": {
                        "Attempts": 0,
                        "Mode": "fail",
                    },
                    "Tasks": [
                        {
                            "Name": "metaflow-task",
                            "Driver": self.driver,
                            "Config": task_config,
                            "Env": {
                                **self.env,
                                "METAFLOW_NOMAD_WORKLOAD": "1",
                            },
                            "Resources": {
                                "CPU": self.cpu,
                                "MemoryMB": self.memory,
                            },
                        }
                    ],
                }
            ],
        }

        if self.region:
            job_spec["Region"] = self.region
        if self.namespace:
            job_spec["Namespace"] = self.namespace

        return job_spec

    def submit(self) -> str:
        """
        Submit the job to Nomad.

        Returns
        -------
        str
            The job ID.
        """
        job_spec = self._build_job_spec()
        self._job_id = self.name
        self._eval_id = self.client.submit(job_spec)
        return self._job_id

    def wait_for_running(self, timeout: int = 600) -> Dict[str, Any]:
        """
        Wait for the job's allocation to start running.

        Returns
        -------
        dict
            The allocation dict.
        """
        if not self._job_id:
            raise NomadException("Job has not been submitted yet.")

        alloc = self.client.wait_for_allocation(self._job_id, timeout=timeout)
        if alloc is None:
            raise NomadException(
                "Timed out waiting for Nomad job '%s' to get an allocation "
                "after %d seconds." % (self._job_id, timeout)
            )
        self._alloc_id = alloc["ID"]
        return alloc

    def wait_for_completion(self, timeout: int = 3600) -> Dict[str, Any]:
        """
        Wait for the batch job to complete.

        Returns
        -------
        dict
            Final allocation dict.
        """
        if not self._job_id:
            raise NomadException("Job has not been submitted yet.")

        return self.client.wait_for_completion(self._job_id, timeout=timeout)

    def get_exit_code(self) -> Optional[int]:
        """
        Get the exit code of the completed job.

        Returns
        -------
        int or None
            The exit code, or None if not available.
        """
        if not self._alloc_id:
            return None

        try:
            alloc = self.client.get_allocation(self._alloc_id)
            task_states = alloc.get("TaskStates", {})
            task_state = task_states.get("metaflow-task", {})
            events = task_state.get("Events", [])

            # Look for the Terminated event which contains the exit code
            for event in reversed(events):
                if event.get("Type") == "Terminated":
                    return event.get("ExitCode", -1)

            # If task failed, return -1
            if task_state.get("Failed", False):
                return 1

            return 0
        except Exception:
            return None

    def get_logs(self, log_type: str = "stdout") -> str:
        """
        Get logs from the job's allocation.

        Parameters
        ----------
        log_type : str
            'stdout' or 'stderr'

        Returns
        -------
        str
            The log content.
        """
        if not self._alloc_id:
            return ""

        return self.client.get_logs(self._alloc_id, log_type=log_type)

    def kill(self):
        """Kill the Nomad job if it's running."""
        if self._job_id:
            try:
                status = self.client.get_job_status(self._job_id)
                if status in ("pending", "running"):
                    self.client.stop_job(self._job_id)
            except Exception:
                pass

    def status(self) -> str:
        """
        Get the current status of the job.

        Returns
        -------
        str
            One of: 'pending', 'running', 'dead', 'unknown'
        """
        if not self._job_id:
            return "unknown"

        try:
            return self.client.get_job_status(self._job_id)
        except Exception:
            return "unknown"

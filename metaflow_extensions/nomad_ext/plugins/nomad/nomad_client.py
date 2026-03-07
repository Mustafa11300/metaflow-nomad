import sys
import time
from typing import Optional, Dict, Any, List

from .nomad_exceptions import NomadException


class NomadClient(object):
    """
    HTTP API client for HashiCorp Nomad using the python-nomad library.
    Replaces the SSH+asyncssh pattern used by the Slurm extension with
    direct HTTP API calls.
    """

    def __init__(
        self,
        address: str = "http://127.0.0.1:4646",
        token: Optional[str] = None,
        region: Optional[str] = None,
        namespace: Optional[str] = "default",
    ):
        try:
            import nomad as nomad_lib

            self._nomad_lib = nomad_lib
        except (NameError, ImportError, ModuleNotFoundError):
            raise NomadException(
                "Could not import module 'python-nomad'.\n\nInstall python-nomad "
                "Python package (https://pypi.org/project/python-nomad/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install python-nomad\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

        kwargs = {"address": address}
        if token:
            kwargs["token"] = token
        if region:
            kwargs["region"] = region
        if namespace:
            kwargs["namespace"] = namespace

        self.client = nomad_lib.Nomad(**kwargs)
        self.address = address
        self.namespace = namespace

    def submit(self, job_spec: Dict[str, Any]) -> str:
        """
        Submit a job to Nomad.

        Parameters
        ----------
        job_spec : dict
            Nomad job specification as a Python dict.

        Returns
        -------
        str
            The evaluation ID returned by Nomad.
        """
        response = self.client.job.register_job({"Job": job_spec})
        return response["EvalID"]

    def get_job_status(self, job_id: str) -> str:
        """
        Get the status of a Nomad job.

        Returns
        -------
        str
            One of: 'pending', 'running', 'dead'
        """
        job = self.client.job.get_job(job_id)
        return job["Status"]

    def get_allocations(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get allocations for a Nomad job.

        Returns
        -------
        list
            List of allocation dicts.
        """
        return self.client.job.get_allocations(job_id)

    def get_allocation(self, alloc_id: str) -> Dict[str, Any]:
        """
        Get details of a specific allocation.
        """
        return self.client.allocation.get_allocation(alloc_id)

    def get_logs(
        self,
        alloc_id: str,
        task_name: str = "metaflow-task",
        log_type: str = "stdout",
    ) -> str:
        """
        Retrieve logs from a Nomad allocation.

        Parameters
        ----------
        alloc_id : str
            The allocation ID.
        task_name : str
            The task name within the allocation.
        log_type : str
            'stdout' or 'stderr'.

        Returns
        -------
        str
            The log content.
        """
        try:
            return self.client.client.stream_logs.stream(
                alloc_id, task_name, log_type, origin="start"
            )
        except Exception:
            # Fallback: try reading the log file directly
            try:
                return self.client.client.read_file(
                    alloc_id, "/alloc/logs/%s.%s.0" % (task_name, log_type)
                )
            except Exception:
                return ""

    def stop_job(self, job_id: str, purge: bool = False) -> str:
        """
        Stop (deregister) a Nomad job.

        Parameters
        ----------
        job_id : str
            The job ID to stop.
        purge : bool
            If True, purge the job from Nomad completely.

        Returns
        -------
        str
            The evaluation ID for the stop operation.
        """
        response = self.client.job.deregister_job(job_id, purge=purge)
        return response

    def wait_for_allocation(
        self, job_id: str, timeout: int = 600, poll_interval: int = 2
    ) -> Optional[Dict[str, Any]]:
        """
        Wait for a job's allocation to be assigned and start running.

        Parameters
        ----------
        job_id : str
            The job ID.
        timeout : int
            Maximum seconds to wait.
        poll_interval : int
            Seconds between polling.

        Returns
        -------
        dict or None
            The allocation dict, or None if timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            allocs = self.get_allocations(job_id)
            if allocs:
                # Return the most recent allocation
                allocs_sorted = sorted(
                    allocs, key=lambda a: a.get("CreateIndex", 0), reverse=True
                )
                latest = allocs_sorted[0]
                client_status = latest.get("ClientStatus", "")
                if client_status in ("running", "complete", "failed"):
                    return latest
            time.sleep(poll_interval)
        return None

    def wait_for_completion(
        self, job_id: str, timeout: int = 3600, poll_interval: int = 5
    ) -> Dict[str, Any]:
        """
        Wait for a batch job to complete (succeed or fail).

        Parameters
        ----------
        job_id : str
            The job ID.
        timeout : int
            Maximum seconds to wait.
        poll_interval : int
            Seconds between polling.

        Returns
        -------
        dict
            Final allocation dict with status info.

        Raises
        ------
        NomadException
            If timeout exceeded.
        """
        start = time.time()
        while time.time() - start < timeout:
            allocs = self.get_allocations(job_id)
            if allocs:
                allocs_sorted = sorted(
                    allocs, key=lambda a: a.get("CreateIndex", 0), reverse=True
                )
                latest = allocs_sorted[0]
                client_status = latest.get("ClientStatus", "")
                if client_status in ("complete", "failed"):
                    return latest
            time.sleep(poll_interval)

        raise NomadException(
            "Timed out waiting for Nomad job '%s' to complete "
            "after %d seconds." % (job_id, timeout)
        )

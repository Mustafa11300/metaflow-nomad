"""
Unit tests for NomadJob — job specification generation and lifecycle.
"""

import pytest
from unittest.mock import MagicMock, patch

from metaflow_extensions.nomad_ext.plugins.nomad.nomad_job import NomadJob, _sanitize_name


class TestSanitizeName:
    """Test the _sanitize_name helper function."""

    def test_alphanumeric_passes_through(self):
        assert _sanitize_name("my-job-123") == "my-job-123"

    def test_special_chars_replaced(self):
        assert _sanitize_name("user@host:flow/step") == "user-host-flow-step"

    def test_underscores_preserved(self):
        assert _sanitize_name("my_job_name") == "my_job_name"

    def test_truncation_at_128(self):
        long_name = "a" * 200
        result = _sanitize_name(long_name)
        assert len(result) == 128

    def test_empty_string(self):
        assert _sanitize_name("") == ""


class TestNomadJobSpec:
    """Test NomadJob._build_job_spec for different configurations."""

    def _make_job(self, **kwargs):
        """Helper to construct a NomadJob with a mocked client."""
        defaults = {
            "client": MagicMock(),
            "name": "test-job",
            "command": ["/bin/bash", "-c", "echo hello"],
            "docker_image": "python:3.11-slim",
            "cpu": 500,
            "memory": 256,
        }
        defaults.update(kwargs)
        return NomadJob(**defaults)

    def test_docker_driver_default(self):
        """Default driver should be docker."""
        job = self._make_job()
        spec = job._build_job_spec()

        task = spec["TaskGroups"][0]["Tasks"][0]
        assert task["Driver"] == "docker"
        assert "image" in task["Config"]
        assert task["Config"]["image"] == "python:3.11-slim"

    def test_raw_exec_driver(self):
        """raw_exec driver should not include Docker image in config."""
        job = self._make_job(driver="raw_exec")
        spec = job._build_job_spec()

        task = spec["TaskGroups"][0]["Tasks"][0]
        assert task["Driver"] == "raw_exec"
        assert "image" not in task["Config"]
        assert task["Config"]["command"] == "/bin/bash"

    def test_job_type_is_batch(self):
        job = self._make_job()
        spec = job._build_job_spec()
        assert spec["Type"] == "batch"

    def test_custom_datacenters(self):
        job = self._make_job(datacenters=["dc1", "dc2"])
        spec = job._build_job_spec()
        assert spec["Datacenters"] == ["dc1", "dc2"]

    def test_default_datacenter_is_dc1(self):
        job = self._make_job()
        spec = job._build_job_spec()
        assert spec["Datacenters"] == ["dc1"]

    def test_cpu_and_memory_resources(self):
        job = self._make_job(cpu=1000, memory=512)
        spec = job._build_job_spec()

        resources = spec["TaskGroups"][0]["Tasks"][0]["Resources"]
        assert resources["CPU"] == 1000
        assert resources["MemoryMB"] == 512

    def test_environment_variables(self):
        job = self._make_job(env={"MY_VAR": "my_value"})
        spec = job._build_job_spec()

        env = spec["TaskGroups"][0]["Tasks"][0]["Env"]
        assert env["MY_VAR"] == "my_value"
        # Should always have the workload marker
        assert env["METAFLOW_NOMAD_WORKLOAD"] == "1"

    def test_region_set_when_provided(self):
        job = self._make_job(region="us-east-1")
        spec = job._build_job_spec()
        assert spec["Region"] == "us-east-1"

    def test_region_not_set_when_none(self):
        job = self._make_job(region=None)
        spec = job._build_job_spec()
        assert "Region" not in spec

    def test_namespace_set(self):
        job = self._make_job(namespace="production")
        spec = job._build_job_spec()
        assert spec["Namespace"] == "production"

    def test_no_command_fallback(self):
        job = self._make_job(command=[])
        spec = job._build_job_spec()

        task = spec["TaskGroups"][0]["Tasks"][0]
        assert task["Config"]["command"] == "/bin/bash"

    def test_restart_policy_is_fail(self):
        """Batch jobs should not restart on failure."""
        job = self._make_job()
        spec = job._build_job_spec()

        restart = spec["TaskGroups"][0]["RestartPolicy"]
        assert restart["Attempts"] == 0
        assert restart["Mode"] == "fail"

    def test_job_name_sanitized(self):
        job = self._make_job(name="user@host:flow/step#1")
        assert job.name == "user-host-flow-step-1"


class TestNomadJobLifecycle:
    """Test NomadJob submit/wait/kill methods."""

    def test_submit_calls_client(self):
        client = MagicMock()
        client.submit.return_value = "eval-123"
        job = NomadJob(
            client=client,
            name="test-job",
            command=["/bin/bash", "-c", "echo hello"],
        )

        job_id = job.submit()
        assert job_id == "test-job"
        assert job.job_id == "test-job"
        client.submit.assert_called_once()

    def test_wait_for_running_success(self):
        client = MagicMock()
        client.submit.return_value = "eval-123"
        client.wait_for_allocation.return_value = {
            "ID": "alloc-abc-def",
            "ClientStatus": "running",
        }

        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        job.submit()
        alloc = job.wait_for_running()

        assert alloc["ClientStatus"] == "running"
        assert job.alloc_id == "alloc-abc-def"

    def test_wait_for_running_timeout(self):
        from metaflow_extensions.nomad_ext.plugins.nomad.nomad_exceptions import (
            NomadException,
        )

        client = MagicMock()
        client.submit.return_value = "eval-123"
        client.wait_for_allocation.return_value = None

        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        job.submit()

        with pytest.raises(NomadException, match="Timed out"):
            job.wait_for_running(timeout=1)

    def test_kill_running_job(self):
        client = MagicMock()
        client.submit.return_value = "eval-123"
        client.get_job_status.return_value = "running"

        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        job.submit()
        job.kill()

        client.stop_job.assert_called_once_with("test")

    def test_kill_dead_job_is_noop(self):
        client = MagicMock()
        client.submit.return_value = "eval-123"
        client.get_job_status.return_value = "dead"

        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        job.submit()
        job.kill()

        client.stop_job.assert_not_called()

    def test_get_exit_code(self):
        client = MagicMock()
        client.submit.return_value = "eval-123"
        client.wait_for_allocation.return_value = {
            "ID": "alloc-123",
            "ClientStatus": "complete",
        }
        client.get_allocation.return_value = {
            "TaskStates": {
                "metaflow-task": {
                    "Events": [
                        {"Type": "Started"},
                        {"Type": "Terminated", "ExitCode": 0},
                    ]
                }
            }
        }

        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        job.submit()
        job.wait_for_running()

        assert job.get_exit_code() == 0

    def test_get_exit_code_failure(self):
        client = MagicMock()
        client.submit.return_value = "eval-123"
        client.wait_for_allocation.return_value = {
            "ID": "alloc-123",
            "ClientStatus": "failed",
        }
        client.get_allocation.return_value = {
            "TaskStates": {
                "metaflow-task": {
                    "Events": [
                        {"Type": "Started"},
                        {"Type": "Terminated", "ExitCode": 1},
                    ]
                }
            }
        }

        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        job.submit()
        job.wait_for_running()

        assert job.get_exit_code() == 1

    def test_status_before_submit(self):
        client = MagicMock()
        job = NomadJob(client=client, name="test", command=["echo", "hi"])
        assert job.status() == "unknown"

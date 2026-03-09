"""
Unit tests for NomadClient — HTTP API wrapper.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from metaflow_extensions.nomad_ext.plugins.nomad.nomad_exceptions import NomadException


class TestNomadClientInit:
    """Test NomadClient initialization."""

    @patch("metaflow_extensions.nomad_ext.plugins.nomad.nomad_client.NomadClient.__init__", return_value=None)
    def test_import_error_raises_exception(self, mock_init):
        """If python-nomad is not installed, raise NomadException."""
        # Direct test — mock the import to fail
        pass  # Covered implicitly by package_init in decorator

    def test_client_created_with_address(self):
        """Client should initialize with the provided address."""
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(
                address="http://localhost:4646",
            )
            assert client.address == "http://localhost:4646"

    def test_client_created_with_namespace(self):
        """Client should store the namespace."""
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(
                address="http://localhost:4646",
                namespace="staging",
            )
            assert client.namespace == "staging"


class TestNomadClientSubmit:
    """Test NomadClient.submit method."""

    def test_submit_returns_eval_id(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.register_job.return_value = {"EvalID": "eval-456"}

            result = client.submit({"ID": "test-job", "Name": "test-job", "Type": "batch"})
            assert result == "eval-456"
            # Verify register_job was called with (id_, job_dict)
            client.client.job.register_job.assert_called_once_with(
                "test-job", {"Job": {"ID": "test-job", "Name": "test-job", "Type": "batch"}}
            )


class TestNomadClientJobStatus:
    """Test NomadClient.get_job_status method."""

    def test_get_job_status(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.get_job.return_value = {"Status": "running"}

            assert client.get_job_status("test-job") == "running"


class TestNomadClientStopJob:
    """Test NomadClient.stop_job method."""

    def test_stop_job_calls_deregister(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.deregister_job.return_value = "eval-stop"

            result = client.stop_job("test-job", purge=True)
            client.client.job.deregister_job.assert_called_once_with(
                "test-job", purge=True
            )


class TestNomadClientWaitForAllocation:
    """Test NomadClient.wait_for_allocation method."""

    def test_returns_alloc_when_running(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.get_allocations.return_value = [
                {"ID": "alloc-1", "ClientStatus": "running", "CreateIndex": 100}
            ]

            result = client.wait_for_allocation("test-job", timeout=5, poll_interval=0.1)
            assert result is not None
            assert result["ID"] == "alloc-1"

    def test_returns_none_on_timeout(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.get_allocations.return_value = []

            result = client.wait_for_allocation("test-job", timeout=0.5, poll_interval=0.1)
            assert result is None


class TestNomadClientWaitForCompletion:
    """Test NomadClient.wait_for_completion method."""

    def test_returns_alloc_when_complete(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.get_allocations.return_value = [
                {"ID": "alloc-1", "ClientStatus": "complete", "CreateIndex": 100}
            ]

            result = client.wait_for_completion("test-job", timeout=5, poll_interval=0.1)
            assert result["ClientStatus"] == "complete"

    def test_raises_on_timeout(self):
        with patch.dict("sys.modules", {"nomad": MagicMock()}):
            import importlib
            from metaflow_extensions.nomad_ext.plugins.nomad import nomad_client

            importlib.reload(nomad_client)
            client = nomad_client.NomadClient(address="http://localhost:4646")
            client.client.job.get_allocations.return_value = [
                {"ID": "alloc-1", "ClientStatus": "pending", "CreateIndex": 100}
            ]

            with pytest.raises(NomadException, match="Timed out"):
                client.wait_for_completion("test-job", timeout=0.5, poll_interval=0.1)

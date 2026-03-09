"""
Standalone Integration Test: Nomad Client <-> Nomad Agent

This script directly tests the NomadClient and NomadJob classes against
a running Nomad dev agent (nomad agent -dev). It does NOT go through
Metaflow's decorator machinery — it proves that the core Nomad integration
layer works correctly.

Prerequisites:
    1. Start Nomad: nomad agent -dev
    2. Install: pip install -e /path/to/metaflow-nomad

Usage:
    python tests/test_integration.py
"""

import sys
import os
import time

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metaflow_extensions.nomad_ext.plugins.nomad.nomad_client import NomadClient
from metaflow_extensions.nomad_ext.plugins.nomad.nomad_job import NomadJob


def print_header(title):
    print("\n" + "=" * 60)
    print("  %s" % title)
    print("=" * 60)


def print_status(label, value, ok=True):
    symbol = "✅" if ok else "❌"
    print("  %s %s: %s" % (symbol, label, value))


def test_nomad_connectivity():
    """Test 1: Can we connect to the Nomad API?"""
    print_header("Test 1: Nomad API Connectivity")
    try:
        client = NomadClient(address="http://127.0.0.1:4646")
        # Try to list jobs — this will fail if Nomad isn't running
        client.client.jobs.get_jobs()
        print_status("Connection", "OK — Nomad is reachable")
        return client
    except Exception as e:
        print_status("Connection", "FAILED — %s" % str(e), ok=False)
        print("\n  💡 Start Nomad with: nomad agent -dev\n")
        return None


def test_raw_exec_job(client):
    """Test 2: Submit a raw_exec batch job and verify it completes."""
    print_header("Test 2: Submit raw_exec Batch Job")

    job = NomadJob(
        client=client,
        name="metaflow-integration-test",
        command=["/bin/bash", "-c", "echo 'Hello from Nomad raw_exec!' && date && hostname"],
        driver="raw_exec",
        cpu=100,
        memory=64,
    )

    spec = job._build_job_spec()
    task = spec["TaskGroups"][0]["Tasks"][0]
    print_status("Driver", task["Driver"])
    print_status("Job Type", spec["Type"])
    print_status("CPU", "%d MHz" % task["Resources"]["CPU"])
    print_status("Memory", "%d MB" % task["Resources"]["MemoryMB"])

    print("\n  Submitting job...")
    job_id = job.submit()
    print_status("Job ID", job_id)

    print("  Waiting for allocation...")
    try:
        alloc = job.wait_for_running(timeout=30)
        alloc_id = job.alloc_id[:12] if job.alloc_id else "unknown"
        print_status("Allocation", alloc_id)
    except Exception as e:
        print_status("Allocation", "FAILED — %s" % str(e), ok=False)
        return False

    print("  Waiting for completion...")
    try:
        final = job.wait_for_completion(timeout=30)
        client_status = final.get("ClientStatus", "unknown")
        print_status("Final Status", client_status, ok=(client_status == "complete"))
    except Exception as e:
        print_status("Completion", "FAILED — %s" % str(e), ok=False)
        return False

    exit_code = job.get_exit_code()
    print_status("Exit Code", str(exit_code), ok=(exit_code == 0))

    # Fetch logs
    stdout = job.get_logs("stdout")
    if stdout:
        print("\n  --- stdout ---")
        for line in str(stdout).strip().split("\n"):
            print("  | %s" % line)
    else:
        print_status("Logs", "No stdout captured (this is normal for raw_exec)", ok=True)

    return exit_code == 0


def test_docker_job_spec():
    """Test 3: Verify Docker job spec generation (without submitting)."""
    print_header("Test 3: Docker Job Spec Generation")

    client_mock = type("MockClient", (), {})()
    job = NomadJob(
        client=client_mock,
        name="docker-spec-test",
        command=["python3", "-c", "print('hello')"],
        driver="docker",
        docker_image="python:3.11-slim",
        cpu=500,
        memory=256,
        env={"MY_VAR": "test_value"},
        datacenters=["dc1", "dc2"],
    )

    spec = job._build_job_spec()
    task = spec["TaskGroups"][0]["Tasks"][0]

    print_status("Driver", task["Driver"])
    print_status("Image", task["Config"]["image"])
    print_status("Command", task["Config"]["command"])
    print_status("Datacenters", str(spec["Datacenters"]))
    print_status("Env METAFLOW_NOMAD_WORKLOAD", task["Env"].get("METAFLOW_NOMAD_WORKLOAD", "missing"))
    print_status("Env MY_VAR", task["Env"].get("MY_VAR", "missing"))

    # Verify
    assert task["Driver"] == "docker", "Expected docker driver"
    assert task["Config"]["image"] == "python:3.11-slim", "Expected python:3.11-slim image"
    assert spec["Datacenters"] == ["dc1", "dc2"], "Expected two datacenters"
    assert task["Env"]["METAFLOW_NOMAD_WORKLOAD"] == "1", "Expected workload marker"

    print_status("All assertions", "PASSED")
    return True


def test_job_cleanup(client):
    """Test 4: Verify job cleanup (kill) works."""
    print_header("Test 4: Job Cleanup")

    job = NomadJob(
        client=client,
        name="metaflow-cleanup-test",
        command=["/bin/bash", "-c", "sleep 60"],
        driver="raw_exec",
        cpu=100,
        memory=64,
    )

    job.submit()
    print_status("Job submitted", job.job_id)

    # Wait briefly for it to start
    time.sleep(2)
    status = job.status()
    print_status("Status before kill", status)

    job.kill()
    time.sleep(1)

    status = job.status()
    print_status("Status after kill", status, ok=(status == "dead"))
    return status == "dead"


def main():
    print("\n🚀 Metaflow-Nomad Integration Tests")
    print("=" * 60)

    results = {}

    # Test 1: Connectivity
    client = test_nomad_connectivity()
    results["connectivity"] = client is not None

    # Test 3: Spec generation (no Nomad needed)
    results["docker_spec"] = test_docker_job_spec()

    if client is None:
        print("\n⚠️  Skipping live tests — Nomad is not running.")
        print("   Start with: nomad agent -dev\n")
    else:
        # Test 2: raw_exec job
        results["raw_exec_job"] = test_raw_exec_job(client)

        # Test 4: Cleanup
        results["cleanup"] = test_job_cleanup(client)

    # Summary
    print_header("Results Summary")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    for name, ok in results.items():
        print_status(name, "PASSED" if ok else "FAILED", ok=ok)

    print("\n  %d/%d tests passed\n" % (passed, total))
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())

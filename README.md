# Metaflow Nomad Extension

Run [Metaflow](https://metaflow.org/) steps as [HashiCorp Nomad](https://www.nomadproject.io/) batch jobs.

This extension adds a `@nomad` step decorator and a `nomad` runtime CLI used by Metaflow to submit and monitor Nomad jobs.

## What This Project Provides

- A Metaflow step decorator: `@nomad(...)`
- Nomad API integration through `python-nomad`
- Nomad batch job spec generation (`docker` and `raw_exec` drivers)
- Job lifecycle handling: submit, wait-for-running, wait-for-completion, logs, stop
- Example flows and unit/integration tests

Note: The current implementation uses `python-nomad` for API interactions. A thin `requests` wrapper is under consideration and may be adopted after mentor alignment.

## Repository Layout

```text
metaflow_extensions/nomad_ext/
  config/                     # Metaflow config defaults (NOMAD_*)
  plugins/nomad/
    nomad_decorator.py        # @nomad StepDecorator implementation
    nomad_cli.py              # `metaflow ... nomad step` runtime command
    nomad_client.py           # python-nomad API wrapper
    nomad_job.py              # Nomad job spec + lifecycle object
    nomad.py                  # orchestration and command construction
examples/
  basic_flow.py
  multi_step_flow.py
tests/
  test_nomad_client.py        # pytest unit tests
  test_nomad_job.py           # pytest unit tests
  test_integration.py         # standalone live integration script
```

## Requirements

- Python 3.8+
- Metaflow
- Nomad CLI and a running Nomad agent/cluster
- Docker only if using the `docker` driver

For local development, `raw_exec` is the easiest path.

## Installation

### Install from PyPI

```bash
pip install metaflow-nomad
```

### Install from source

```bash
git clone https://github.com/Mustafa11300/metaflow-nomad.git
cd metaflow-nomad
pip install -e .
```

## Quick Start (Local)

### 1. Start a Nomad dev agent

```bash
nomad agent -dev
```

### 2. Run an example flow

In a second terminal:

```bash
cd /path/to/metaflow-nomad
PYTHONPATH=/path/to/metaflow-nomad \
METAFLOW_NOMAD_DRIVER=raw_exec \
python -m examples.basic_flow run
```

Run the DAG example:

```bash
cd /path/to/metaflow-nomad
PYTHONPATH=/path/to/metaflow-nomad \
METAFLOW_NOMAD_DRIVER=raw_exec \
python -m examples.multi_step_flow run
```

### 3. Optional: inspect jobs

```bash
nomad status
```

## Minimal Usage Example

```python
from metaflow import FlowSpec, step, nomad

class NomadFlow(FlowSpec):

    @nomad(cpu=500, memory=256, driver="raw_exec")
    @step
    def start(self):
        print("Hello from Nomad")
        self.next(self.end)

    @step
    def end(self):
        print("Done")

if __name__ == "__main__":
    NomadFlow()
```

Run:

```bash
PYTHONPATH=/path/to/metaflow-nomad python -m examples.basic_flow run
```

## Configuration

Set through Metaflow config / environment variables:

| Variable | Description | Default |
|---|---|---|
| `METAFLOW_NOMAD_ADDRESS` | Nomad API URL | `http://127.0.0.1:4646` |
| `METAFLOW_NOMAD_TOKEN` | ACL token | unset |
| `METAFLOW_NOMAD_REGION` | Nomad region | unset |
| `METAFLOW_NOMAD_NAMESPACE` | Nomad namespace | `default` |
| `METAFLOW_NOMAD_DOCKER_IMAGE` | Default container image | `python:3.11-slim` |
| `METAFLOW_NOMAD_DRIVER` | Driver (`docker` or `raw_exec`) | `docker` |
| `METAFLOW_NOMAD_DATACENTERS` | Comma-separated datacenters | `dc1` |

Decorator parameters override defaults per step.

## `@nomad` Decorator Parameters

| Parameter | Type | Meaning |
|---|---|---|
| `address` | `str` | Nomad API address |
| `token` | `str` | ACL token |
| `region` | `str` | Nomad region |
| `namespace` | `str` | Nomad namespace |
| `docker_image` | `str` | Image for docker driver |
| `cpu` | `int` | CPU (MHz) |
| `memory` | `int` | Memory (MB) |
| `driver` | `str` | `docker` or `raw_exec` |
| `datacenters` | `str` or `list[str]` | Target datacenter(s) |

Planned/stretch API note: `gpu` is discussed in the proposal as a potential future parameter and is not part of the current implemented decorator contract.

## Driver Guidance

### `raw_exec`

- Best for local development and debugging
- Does not require Docker
- Runs directly on Nomad client host

### `docker`

- Preferred for portable and production-like execution
- Requires Docker driver availability in Nomad

## Architecture Overview

- `NomadDecorator` wires into Metaflow step lifecycle and rewrites runtime CLI to `nomad step`.
- `Nomad` builds task command/environment and constructs `NomadJob`.
- `NomadJob` builds a Nomad job spec dict and handles lifecycle operations.
- `NomadClient` wraps `python-nomad` API calls.

This separation allows unit-testing job spec generation without talking to a live Nomad cluster.

## Testing

### Unit tests (pytest)

```bash
pytest tests/test_nomad_job.py tests/test_nomad_client.py -v
```

### Live integration script

Requires a running Nomad dev agent:

```bash
nomad agent -dev
python tests/test_integration.py
```

## Development Workflow

```bash
git clone https://github.com/Mustafa11300/metaflow-nomad.git
cd metaflow-nomad
python -m venv .venv
source .venv/bin/activate
pip install -e . pytest metaflow
```

Then run examples/tests as shown above.

## Troubleshooting

### `ImportError: cannot import name 'nomad' from 'metaflow'`

Use module execution from repo root and set `PYTHONPATH` when running from source checkout:

```bash
PYTHONPATH=/path/to/metaflow-nomad python -m examples.basic_flow run
```

### Nomad not reachable

```bash
nomad status
```

If it fails, start local dev agent:

```bash
nomad agent -dev
```

### `python-nomad` import error

```bash
pip install python-nomad
```

### Driver issues

If Docker is unavailable locally, force `raw_exec`:

```bash
export METAFLOW_NOMAD_DRIVER=raw_exec
```

## Current Limitations

- `@parallel` with `@nomad` is not supported.
- Integration coverage is currently a standalone script (`tests/test_integration.py`), not yet pytest-marked integration tests.
- Migration of integration coverage to pytest-marked integration tests is planned for Phase 1, Weeks 5-6.

## License

Apache License 2.0

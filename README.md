# Metaflow Nomad Extension

Execute [Metaflow](https://metaflow.org/) steps as [HashiCorp Nomad](https://www.nomadproject.io/) batch jobs.

## Installation

```bash
pip install metaflow-nomad
```

Or install from source:

```bash
git clone https://github.com/YOUR_USERNAME/metaflow-nomad.git
cd metaflow-nomad
pip install -e .
```

## Prerequisites

- **Nomad cluster** — either a production cluster or `nomad agent -dev` for local testing
- **Docker** (optional) — for the Docker task driver, or use `raw_exec` for local development
- **Metaflow** — `pip install metaflow`

## Quick Start

### 1. Start a local Nomad dev agent

```bash
nomad agent -dev
```

### 2. Run a flow with the `@nomad` decorator

```python
from metaflow import FlowSpec, step, nomad

class NomadFlow(FlowSpec):

    @nomad(cpu=500, memory=256)
    @step
    def start(self):
        print("Hello from Nomad!")
        self.next(self.end)

    @step
    def end(self):
        print("Done!")

if __name__ == "__main__":
    NomadFlow()
```

Run it:
```bash
python nomad_flow.py run
```

### 3. Use `raw_exec` for local testing (no Docker needed)

```bash
export METAFLOW_NOMAD_DRIVER=raw_exec
python nomad_flow.py run
```

Or specify per-step:
```python
@nomad(driver="raw_exec", cpu=500, memory=256)
```

## Configuration

Configuration can be set via environment variables or Metaflow config:

| Environment Variable | Description | Default |
|---|---|---|
| `METAFLOW_NOMAD_ADDRESS` | Nomad API address | `http://127.0.0.1:4646` |
| `METAFLOW_NOMAD_TOKEN` | ACL token (if ACLs enabled) | None |
| `METAFLOW_NOMAD_REGION` | Nomad region | None |
| `METAFLOW_NOMAD_NAMESPACE` | Nomad namespace | `default` |
| `METAFLOW_NOMAD_DOCKER_IMAGE` | Default Docker image | `python:3.11-slim` |
| `METAFLOW_NOMAD_DRIVER` | Task driver (`docker` or `raw_exec`) | `docker` |
| `METAFLOW_NOMAD_DATACENTERS` | Comma-separated datacenters | `dc1` |

Or pass directly to the decorator:
```python
@nomad(address="http://nomad.example.com:4646", cpu=1000, memory=512, driver="raw_exec")
```

## Decorator Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `address` | str | from config | Nomad API address |
| `token` | str | from config | ACL token |
| `region` | str | from config | Nomad region |
| `namespace` | str | from config | Nomad namespace |
| `docker_image` | str | `python:3.11-slim` | Docker image for the task |
| `cpu` | int | 500 | CPU in MHz |
| `memory` | int | 256 | Memory in MB |
| `driver` | str | `docker` | Task driver (`docker` or `raw_exec`) |
| `datacenters` | list | `["dc1"]` | Target datacenters |

## Examples

### Basic flow (`examples/basic_flow.py`)
A single step running on Nomad.

### Multi-step DAG (`examples/multi_step_flow.py`)
A branching flow: `start → [process_a, process_b] → join → end`

## Architecture

This extension follows the same pattern as the [metaflow-slurm](https://github.com/outerbounds/metaflow-slurm) extension:

- **`NomadDecorator`** — `StepDecorator` subclass that hooks into Metaflow's lifecycle
- **`NomadClient`** — HTTP API wrapper using `python-nomad`
- **`NomadJob`** — Generates batch job specs and manages job lifecycle (supports `docker` and `raw_exec` drivers)
- **`Nomad`** — Orchestration class that constructs commands and runs jobs

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest

# Unit tests (no Nomad required)
pytest tests/test_nomad_job.py tests/test_nomad_client.py -v

# Integration tests (requires: nomad agent -dev)
python tests/test_integration.py
```

### Local Development Setup

```bash
cd metaflow-nomad
pip install -e .
nomad agent -dev
export METAFLOW_NOMAD_DRIVER=raw_exec
python examples/basic_flow.py run
```

## License

Apache License 2.0

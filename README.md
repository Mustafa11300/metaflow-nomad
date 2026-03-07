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
- **Docker** — Nomad's Docker task driver must be enabled
- **Metaflow** — `pip install metaflow`

## Basic Usage

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

## Configuration

Configuration can be set via environment variables or Metaflow config:

| Environment Variable | Description | Default |
|---|---|---|
| `METAFLOW_NOMAD_ADDRESS` | Nomad API address | `http://127.0.0.1:4646` |
| `METAFLOW_NOMAD_TOKEN` | ACL token (if ACLs enabled) | None |
| `METAFLOW_NOMAD_REGION` | Nomad region | None |
| `METAFLOW_NOMAD_NAMESPACE` | Nomad namespace | `default` |
| `METAFLOW_NOMAD_DOCKER_IMAGE` | Default Docker image | `python:3.11-slim` |

Or pass directly to the decorator:
```python
@nomad(address="http://nomad.example.com:4646", cpu=1000, memory=512)
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

## Local Development

Start a local Nomad dev agent:
```bash
nomad agent -dev
```

Verify it's running:
```bash
nomad status
```

Install the extension in dev mode:
```bash
cd metaflow-nomad
pip install -e .
```

## Architecture

This extension follows the same pattern as the [metaflow-slurm](https://github.com/outerbounds/metaflow-slurm) extension:

- **`NomadDecorator`** — `StepDecorator` subclass that hooks into Metaflow's lifecycle
- **`NomadClient`** — HTTP API wrapper using `python-nomad` (replaces Slurm's SSH+asyncssh)
- **`NomadJob`** — Generates batch job specs and manages job lifecycle
- **`Nomad`** — Orchestration class that constructs commands and runs jobs

## License

Apache License 2.0

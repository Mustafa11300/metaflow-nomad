from metaflow.metaflow_config_funcs import from_conf

###
# Nomad configuration
###

# Address of the Nomad API (e.g. http://127.0.0.1:4646)
NOMAD_ADDRESS = from_conf("NOMAD_ADDRESS", default="http://127.0.0.1:4646")

# ACL token for Nomad (optional, required if ACLs are enabled)
NOMAD_TOKEN = from_conf("NOMAD_TOKEN", default=None)

# Nomad region to submit jobs to (optional)
NOMAD_REGION = from_conf("NOMAD_REGION", default=None)

# Nomad namespace to submit jobs to (optional)
NOMAD_NAMESPACE = from_conf("NOMAD_NAMESPACE", default="default")

# Default Docker image for Nomad tasks
NOMAD_DOCKER_IMAGE = from_conf("NOMAD_DOCKER_IMAGE", default="python:3.11-slim")

# Task driver to use: 'docker' or 'raw_exec'
# raw_exec is useful for local development with `nomad agent -dev`
NOMAD_DRIVER = from_conf("NOMAD_DRIVER", default="docker")

# Comma-separated list of datacenters to target (default: dc1)
NOMAD_DATACENTERS = from_conf("NOMAD_DATACENTERS", default="dc1")

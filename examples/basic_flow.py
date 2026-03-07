"""
Example: Basic Nomad Flow

Run a simple Metaflow flow on a local Nomad cluster.

Prerequisites:
    1. Start Nomad: nomad agent -dev
    2. Install: pip install -e /path/to/metaflow-nomad

Usage:
    python examples/basic_flow.py run
"""

from metaflow import FlowSpec, step, nomad


class BasicNomadFlow(FlowSpec):
    """A simple flow that runs a step on Nomad."""

    @nomad(cpu=500, memory=256)
    @step
    def start(self):
        import platform

        self.hostname = platform.node()
        print("Hello from Nomad!")
        print("Running on: %s" % self.hostname)
        print("Python version: %s" % platform.python_version())
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed!")
        print("Step 'start' ran on host: %s" % self.hostname)


if __name__ == "__main__":
    BasicNomadFlow()

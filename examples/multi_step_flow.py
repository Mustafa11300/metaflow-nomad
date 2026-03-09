"""
Example: Multi-Step Nomad DAG Flow

Demonstrates a Metaflow flow with multiple steps running on Nomad,
showcasing branching, data passing between steps, and step-level
resource configuration.

Prerequisites:
    1. Start Nomad: nomad agent -dev
    2. Install: pip install -e /path/to/metaflow-nomad
    3. Set driver: export METAFLOW_NOMAD_DRIVER=raw_exec  (for local testing)

Usage:
    python examples/multi_step_flow.py run
"""

from metaflow import FlowSpec, step, nomad


class MultiStepNomadFlow(FlowSpec):
    """
    A multi-step flow demonstrating Nomad integration with a DAG:

        start → [process_a, process_b] → join → end

    Each step runs as a separate Nomad batch job.
    """

    @nomad(cpu=200, memory=128)
    @step
    def start(self):
        """Initialize the flow with some data."""
        import platform

        self.data = list(range(10))
        self.hostname = platform.node()
        print("🚀 Starting flow on: %s" % self.hostname)
        print("   Generated data: %s" % self.data)
        self.next(self.process_a, self.process_b)

    @nomad(cpu=300, memory=128)
    @step
    def process_a(self):
        """Process path A: compute sum."""
        self.result_a = sum(self.data)
        print("📊 Path A: Sum = %d" % self.result_a)
        self.next(self.join)

    @nomad(cpu=300, memory=128)
    @step
    def process_b(self):
        """Process path B: compute product of first 5 elements."""
        product = 1
        for x in self.data[:5]:
            if x != 0:
                product *= x
        self.result_b = product
        print("📊 Path B: Product = %d" % self.result_b)
        self.next(self.join)

    @nomad(cpu=200, memory=128)
    @step
    def join(self, inputs):
        """Join results from both branches."""
        self.sum_result = inputs.process_a.result_a
        self.product_result = inputs.process_b.result_b
        print("🔗 Joined results:")
        print("   Sum (A):     %d" % self.sum_result)
        print("   Product (B): %d" % self.product_result)
        self.next(self.end)

    @step
    def end(self):
        """Report final results."""
        print("✅ Flow completed!")
        print("   Sum result:     %d" % self.sum_result)
        print("   Product result: %d" % self.product_result)


if __name__ == "__main__":
    MultiStepNomadFlow()

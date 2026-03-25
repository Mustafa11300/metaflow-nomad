import sys
import json
import traceback

from metaflow._vendor import click

from metaflow import util, current
from metaflow import decorators as deco_module
from metaflow.datastore import FlowDataStore
from metaflow.metadata_provider import MetadataProvider

from .nomad import Nomad
from .nomad_exceptions import NomadException


@click.group()
def cli():
    pass


@cli.group(help="Commands for running steps on Nomad.")
def nomad():
    pass


@nomad.command(help="Execute a single step on a Nomad cluster.")
@click.argument("step-name")
@click.argument("code-package-metadata")
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--address", default=None, help="Nomad API address")
@click.option("--token", default=None, help="Nomad ACL token")
@click.option("--region", default=None, help="Nomad region")
@click.option("--nomad-namespace", default=None, help="Nomad namespace")
@click.option("--docker-image", default=None, help="Docker image for the task")
@click.option("--cpu", default=500, type=int, help="CPU in MHz")
@click.option("--memory", default=256, type=int, help="Memory in MB")
@click.option("--datacenters", default=None, help="Nomad datacenters")
@click.option("--run-time-limit", default=None, type=int, help="Time limit in seconds")
@click.option("--run-id", help="Passed to the top-level 'step'.")
@click.option("--task-id", help="Passed to the top-level 'step'.")
@click.option("--input-paths", help="Passed to the top-level 'step'.")
@click.option("--split-index", help="Passed to the top-level 'step'.")
@click.option("--clone-path", help="Passed to the top-level 'step'.")
@click.option("--clone-run-id", help="Passed to the top-level 'step'.")
@click.option("--tag", multiple=True, default=None, help="Passed to the top-level 'step'.")
@click.option("--namespace", default=None, help="Passed to the top-level 'step'.")
@click.option("--retry-count", default=0, help="Passed to the top-level 'step'.")
@click.option("--max-user-code-retries", default=0, help="Passed to the top-level 'step'.")
@click.option("--ubf-context", default=None)
@click.option(
    "--driver",
    default="docker",
    type=click.Choice(["docker", "raw_exec"]),
    help="Nomad task driver (docker or raw_exec)",
)
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_metadata,
    code_package_sha,
    code_package_url,
    address,
    token,
    region,
    nomad_namespace,
    docker_image,
    cpu,
    memory,
    datacenters,
    run_time_limit,
    driver,
    **kwargs,
):
    def echo(msg, stream="stderr", **kwargs):
        if stream == "stderr":
            click.secho(msg, **kwargs, err=True)
        else:
            click.echo(msg, **kwargs)

    node = ctx.obj.graph[step_name]
    echo(
        "    Launching Nomad job for step *%s* (run-id: %s)..."
        % (step_name, kwargs.get("run_id")),
        fg="magenta",
        bold=True,
    )

    # Build Nomad access params
    nomad_access_params = {
        "address": address,
        "token": token,
        "region": region,
        "namespace": nomad_namespace,
        "docker_image": docker_image,
        "cpu": cpu,
        "memory": memory,
        "driver": driver,
        "datacenters": datacenters,
    }

    # Get the metadata provider and datastore
    metadata = ctx.obj.metadata
    flow_datastore = ctx.obj.flow_datastore
    environment = ctx.obj.environment

    # Create the Nomad orchestrator
    nomad_runner = Nomad(
        datastore=flow_datastore,
        metadata=metadata,
        environment=environment,
        nomad_access_params=nomad_access_params,
    )

    # Build the step CLI command that will be run inside the Nomad container
    top_args = " ".join(util.dict_to_cli_options(ctx.parent.parent.params))
    step_args = " ".join(util.dict_to_cli_options(kwargs))

    entrypoint = ctx.obj.entrypoint
    if isinstance(entrypoint, (list, tuple)):
        entrypoint = " ".join(entrypoint)

    step_cli = "{entrypoint} {top_args} step {step} {step_args}".format(
        entrypoint=entrypoint,
        top_args=top_args,
        step=step_name,
        step_args=step_args,
    )

    # Task spec for mflog
    task_spec = {
        "flow_name": ctx.obj.flow.name,
        "run_id": kwargs.get("run_id"),
        "step_name": step_name,
        "task_id": kwargs.get("task_id"),
        "retry_count": kwargs.get("retry_count", 0),
    }

    # Attributes for job naming
    attrs = {
        "metaflow.user": util.get_username(),
        "metaflow.flow_name": ctx.obj.flow.name,
        "metaflow.run_id": kwargs.get("run_id"),
        "metaflow.step_name": step_name,
        "metaflow.task_id": kwargs.get("task_id"),
        "metaflow.retry_count": kwargs.get("retry_count", 0),
    }

    env = {}

    try:
        # Create and run the job
        job = nomad_runner.create_job(
            step_name=step_name,
            step_cli=step_cli,
            task_spec=task_spec,
            code_package_sha=code_package_sha,
            code_package_url=code_package_url,
            code_package_ds=flow_datastore.TYPE,
            docker_image=docker_image,
            cpu=cpu,
            memory=memory,
            run_time_limit=run_time_limit,
            env=env,
            attrs=attrs,
            driver=driver,
        )

        exit_code = nomad_runner.run_job(job, timeout=run_time_limit or 3600)

        if exit_code != 0:
            echo(
                "    Nomad task failed with exit code %d." % exit_code,
                fg="red",
                bold=True,
            )
            sys.exit(exit_code)
        else:
            echo(
                "    Nomad task completed successfully.",
                fg="green",
                bold=True,
            )

    except NomadException as e:
        echo("    Nomad error: %s" % str(e), fg="red")
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        echo("    Unexpected error: %s" % str(e), fg="red")
        traceback.print_exc()
        sys.exit(1)

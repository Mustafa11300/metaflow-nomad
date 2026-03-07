import sys
import json
import traceback

import click

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
@click.argument("code-package-sha")
@click.argument("code-package-url")
@click.option("--address", default=None, help="Nomad API address")
@click.option("--token", default=None, help="Nomad ACL token")
@click.option("--region", default=None, help="Nomad region")
@click.option("--namespace", default=None, help="Nomad namespace")
@click.option("--docker-image", default=None, help="Docker image for the task")
@click.option("--cpu", default=500, type=int, help="CPU in MHz")
@click.option("--memory", default=256, type=int, help="Memory in MB")
@click.option("--run-time-limit", default=None, type=int, help="Time limit in seconds")
@click.pass_context
def step(
    ctx,
    code_package_sha,
    code_package_url,
    address,
    token,
    region,
    namespace,
    docker_image,
    cpu,
    memory,
    run_time_limit,
    **kwargs,
):
    def echo(msg, stream="stderr", **kwargs):
        if stream == "stderr":
            click.secho(msg, **kwargs, err=True)
        else:
            click.echo(msg, **kwargs)

    node = ctx.obj.graph[ctx.obj.step_name]
    echo(
        "    Launching Nomad job for step *%s* (run-id: %s)..."
        % (ctx.obj.step_name, ctx.obj.run_id),
        fg="magenta",
        bold=True,
    )

    # Build Nomad access params
    nomad_access_params = {
        "address": address,
        "token": token,
        "region": region,
        "namespace": namespace,
        "docker_image": docker_image,
        "cpu": cpu,
        "memory": memory,
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
    top_args = " ".join(
        util.dict_to_cli_options(ctx.obj.top_cli_options)
    )
    input_paths = kwargs.get("input_paths")
    split_index = kwargs.get("split_index")

    step_args = " ".join(
        util.dict_to_cli_options(ctx.obj.task_cli_options)
    )

    step_cli = "{entrypoint} {top_args} step {step_args}".format(
        entrypoint=ctx.obj.entrypoint,
        top_args=top_args,
        step_args=step_args,
    )

    # Task spec for mflog
    task_spec = {
        "flow_name": ctx.obj.flow.name,
        "run_id": ctx.obj.run_id,
        "step_name": ctx.obj.step_name,
        "task_id": ctx.obj.task_id,
        "retry_count": ctx.obj.retry_count,
    }

    # Attributes for job naming
    attrs = {
        "metaflow.user": util.get_username(),
        "metaflow.flow_name": ctx.obj.flow.name,
        "metaflow.run_id": ctx.obj.run_id,
        "metaflow.step_name": ctx.obj.step_name,
        "metaflow.task_id": ctx.obj.task_id,
        "metaflow.retry_count": ctx.obj.retry_count,
    }

    env = {}

    try:
        # Create and run the job
        job = nomad_runner.create_job(
            step_name=ctx.obj.step_name,
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

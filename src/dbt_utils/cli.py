from dbt.cli.main import dbtRunner
from dbt.cli.exceptions import DbtUsageException
import os
import sys
import shutil
import asyncio
from functools import wraps
from yaspin import yaspin


from pathlib import Path
import click

from dbt_utils.materialize_metrics import get_metric_queries_as_dbt_vars


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def exit_with_error(msg: str):
    click.secho(msg, fg="red")
    sys.exit(1)


class CatchAllGroup(click.Group):
    """
    Small wrapper that forwards all unknown commands to a callback function.
    This allows us to e.g. proxy unknown commands to another process.
    """

    def get_command(self, ctx, cmd_name):
        cmd = super().get_command(ctx, cmd_name)
        if cmd is not None:
            return cmd

        return click.Command(
            cmd_name,
            callback=parse_and_proxy,
            context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
            params=[click.Option(["--target"], type=str, required=False)],
        )


@click.group(
    cls=CatchAllGroup,
    invoke_without_command=True,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
        help_option_names=["-dbtuh", "--dbt-utils-help"],
    ),
)
@click.option("--debug", "-d", is_flag=True)
# @click.version_option(version=__version__)
@click.pass_context
def cli(ctx, debug):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if __name__ == "main"` block below)
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug

    if (
        ctx.invoked_subcommand is None
        or ctx.invoked_subcommand not in ["compile", "docs", "show", "run", "test"]
        or ctx.invoked_subcommand.startswith("--")
        or ctx.invoked_subcommand.startswith("-")
    ):
        # We want to proxy this directly to dbt as we want to
        # be as invisible as possible for the user.
        args = (
            [ctx.invoked_subcommand, *ctx.args]
            if ctx.invoked_subcommand is not None
            else ctx.args
        )
        res = dbtRunner().invoke(args)

        # Exit required to prevent from moving into the cath-all command.
        sys.exit(0 if res.success else 1)


@cli.command()
@click.option(
    "--macros_dir",
    type=click.Path(exists=True),
    required=False,
    default=Path("./macros"),
)
def init(macros_dir):
    invocation_path = Path(os.path.abspath(sys.argv[0]))
    # Shadow dbt executable with dbt-utils executable
    shutil.copy(invocation_path, invocation_path.parent / "dbt")

    # Install the dbt_metric_utils_materialize.sql macro into the dbt project.
    if not macros_dir.exists:
        exit_with_error(
            "No macros directory found. Please create a macros directory in the root of your dbt project."
        )

    shutil.copy(Path(__file__).parent / "dbt_utils_metric_materialize.sql", macros_dir)

    click.secho(
        "Replaced dbt executable with dbt-utils executable."
        + " All calls to dbt will be intercepted by dbt-utils and proxied to dbt."
        + " You may need to restart your shell for the desired effect.",
        fg="green",
    )


@click.pass_context
def parse_and_proxy(ctx, target):
    with yaspin(text="Building metric queries...", color="yellow") as spinner:
        manifest, metric_query_as_vars = get_metric_queries_as_dbt_vars(target)
        spinner.ok("✅ ")
        res = dbtRunner(manifest=manifest).invoke(
            [ctx.command.name, *ctx.args, "--vars", metric_query_as_vars]
        )

        match res.exception:
            case DbtUsageException():
                exit_with_error(str(res.exception))
            case _:
                pass


if __name__ == "__main__":
    cli()

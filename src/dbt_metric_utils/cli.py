import os
import shutil
import sys
import yaml
from pathlib import Path

import click
from dbt.cli.exceptions import DbtUsageException
from dbt.cli.main import dbtRunner

from dbt_metric_utils.materialize_metrics import get_metric_queries_as_dbt_vars


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
@click.pass_context
def cli(ctx, debug):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if __name__ == "main"` block below)
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    # Use sys to extract commands. Click is working against us here because we use this catch-all group.
    _args = sys.argv[1:]

    if (
        # Plain invocation of dbt
        len(_args) == 0
        # Something like dbt --help
        or _args[0].startswith("--")
        or _args[0].startswith("-")
        # Commands that don't require compilation.
        # Docs generation requires compilation but serving doesn't. If we do, we reset the lineage again.
        or (
            _args[0] not in ["compile", "show", "run", "test"]
            and (_args[0] == "docs" and _args[1] != "generate")
        )
        or (_args[0] in ["clean","deps"])  # TODO: there may be other subcommands which should be passed through
    ):
        res = dbtRunner().invoke(_args)

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

    shutil.copy(Path(__file__).parent / "dbt_metric_utils_materialize.sql", macros_dir)

    click.secho(
        "Replaced dbt executable with dbt-utils executable."
        + " All calls to dbt will be intercepted by dbt-utils and proxied to dbt."
        + " You may need to restart your shell for the desired effect.",
        fg="green",
    )


@click.pass_context
def parse_and_proxy(ctx, target):
    # TODO: add logging (preferably in a similar form to dbt)
    manifest, metric_query_as_vars = get_metric_queries_as_dbt_vars(target)

    # Append vars in the input args to the metric_query_as_vars
    vars_dict = yaml.safe_load(metric_query_as_vars)

    provided_vars = {}
    provided_vars_ind = ctx.args.index("--vars") if "--vars" in ctx.args else None
    if provided_vars_ind is not None:
        provided_vars = ctx.args[provided_vars_ind+1]
        provided_vars = yaml.safe_load(provided_vars)  # Should work for both yaml and json
        ctx.args = ctx.args[0:provided_vars_ind]+ctx.args[provided_vars_ind+2:]

    for k,v in provided_vars.items():
        vars_dict[k] = v
    res = dbtRunner(manifest=manifest).invoke(
        [ctx.command.name, *ctx.args, "--vars", yaml.dump(vars_dict)]
    )

    match res.exception:
        case DbtUsageException():
            exit_with_error(str(res.exception))
        case _:
            pass


if __name__ == "__main__":
    cli()

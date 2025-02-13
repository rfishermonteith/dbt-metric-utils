import sys
import os
import shutil

import click
import yaml
from pathlib import Path

from dbt.cli.exceptions import DbtUsageException
import dbt.cli.main as dbt_main
from dbt.cli.main import dbtRunner

from dbt_metric_utils.materialize_metrics import get_metric_queries_as_dbt_vars


def exit_with_error(msg: str):
    click.secho(msg, fg="red")
    sys.exit(1)


@click.command("init-dbtmu")
@click.option(
    "--macros_dir",
    type=click.Path(exists=True),
    required=False,
    default=Path("./macros"),
)
def init_dbtmu(macros_dir):
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


def cli():
    # Do we need to intervene, or can we pass directly to dbt?
    _args = sys.argv[1:]

    if (
            len(_args) == 0  # Plain invocation of dbt
            # Something like dbt --help
            or _args[0].startswith("-")
            # Commands that require compilation.
            # Docs generation requires compilation but serving doesn't. If we do, we reset the lineage again.
            or (
            _args[0] not in ["compile", "show", "run", "test", "build"]
            and (_args[0] == "docs" and _args[1] != "generate")
    )
            or (_args[0] in ["clean", "deps", "init-dbtmu", "init"])
            # TODO: there may be other subcommands which should be passed through
    ):
        # We don't need to intervene
        return dbt_main.cli()
    else:
        ctx = dbt_main.cli.make_context("cli", _args)
        subcommand_name = _args[0]
        # Ensure that subcommand is valid
        if not dbt_main.cli.get_command(ctx, subcommand_name):
            ctx.fail(f'No such command "{subcommand_name}".')

        sub_cmd = dbt_main.cli.get_command(ctx, subcommand_name)
        sub_ctx = sub_cmd.make_context(subcommand_name, ctx.args, parent=ctx)
        target = sub_ctx.params.get('target')

        # Update the dependencies in the manifest and get the compilied queries
        manifest, metric_query_as_vars = get_metric_queries_as_dbt_vars(target)

        # Append vars in the input args to the metric_query_as_vars
        vars_dict = yaml.safe_load(metric_query_as_vars)
        provided_vars = sub_ctx.params.get('vars')
        if provided_vars:
            for k, v in provided_vars.items():
                vars_dict[k] = v

        # We don't remove vars from _args, since they'll be overridden by the explicit --vars
        res = dbtRunner(manifest=manifest).invoke([_args[0], *_args[1:], "--vars", yaml.dump(vars_dict)])

        match res.exception:
            case DbtUsageException():
                exit_with_error(str(res.exception))
            case _:
                pass

        sys.exit(0 if res.success else 1)


dbt_main.cli.add_command(init_dbtmu)

if __name__ == '__main__':
    sys.exit(cli())

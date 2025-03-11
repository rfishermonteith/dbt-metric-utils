import sys
import os
import shutil
from pathlib import Path

import click
import yaml

from dbt.cli.exceptions import DbtUsageException
import dbt.cli.main as dbt_main
from dbt.cli.main import dbtRunner
from dbt_metric_utils.materialize_metrics import get_metric_queries_as_dbt_vars


def exit_with_error(msg: str) -> None:
    """
    Print an error message in red and exit the program.

    Args:
        msg: The error message to display.
    """
    click.secho(msg, fg="red")
    sys.exit(1)


@click.command("init-dbtmu")
@click.option(
    "--macros_dir",
    type=click.Path(exists=True),
    required=False,
    default=Path("./macros"),
)
def init_dbtmu(macros_dir) -> None:
    """
    Initializes dbt_metric_utils by copying the dbt executable and installing the required SQL macro into the
    specified macros directory.

    Args:
        macros_dir: The directory where dbt macros are stored.
    """

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


def should_intercept_command(args: list) -> bool:
    """
    Determines whether the provided CLI arguments require intervention (i.e.,
    updating the manifest with metric queries) or if they should be passed through.

    Args:
        args: The list of CLI arguments (excluding the executable name).

    Returns:
        True if the command requires intervention; False if it should be passed through.
    """
    if not args:
        return False  # No arguments means plain invocation

    command = args[0]

    # If the command starts with a dash (e.g., --help) or is one that does not require metric intervention,
    # then we do not intercept.
    if command.startswith("-") or command in ["clean", "deps", "init-dbtmu", "init"]:
        return False

    # For commands that require compilation, we need to intervene.
    # The list of commands requiring intervention.
    intervention_commands = {"compile", "show", "run", "test", "build"}
    if command in intervention_commands:
        return True

    # Special case for docs: only intercept if the docs subcommand is 'generate'
    if command == "docs":
        # Ensure there's a second argument before checking
        if len(args) > 1 and args[1] == "generate":
            return True
        else:
            return False

    return False


def cli():
    """
    The main CLI entrypoint that intercepts certain dbt commands to inject metric queries
    into the dbt manifest, while passing through other commands directly to dbt.

    Returns:
        An exit code (0 for success, non-zero for failure).
    """
    # Do we need to intervene, or can we pass directly to dbt?
    _args = sys.argv[1:]

    # If the command does not require intervention, defer to the original dbt CLI.
    if not should_intercept_command(_args):
        return dbt_main.cli()

    # Create a Click context from the current CLI arguments.
    ctx = dbt_main.cli.make_context("cli", _args)
    subcommand_name = _args[0]

    # Validate the subcommand.
    if not dbt_main.cli.get_command(ctx, subcommand_name):
        ctx.fail(f'No such command "{subcommand_name}".')

    sub_cmd = dbt_main.cli.get_command(ctx, subcommand_name)
    sub_ctx = sub_cmd.make_context(subcommand_name, ctx.args, parent=ctx)
    target = sub_ctx.params.get("target")

    # Update the manifest with metric queries using the provided target and get the compilied queries
    manifest, metric_query_as_vars = get_metric_queries_as_dbt_vars(target)

    # Load the metric queries as a dictionary.
    metric_vars = yaml.safe_load(metric_query_as_vars) or {}
    # Merge any variables provided explicitly in the CLI.
    provided_vars = sub_ctx.params.get("vars")
    if provided_vars:
        metric_vars.update(provided_vars)

    # Build the command-line arguments for invoking dbt.
    # We append the merged variables as a YAML dump.
    invoke_args = [_args[0], *_args[1:], "--vars", yaml.dump(metric_vars)]
    res = dbtRunner(manifest=manifest).invoke(invoke_args)

    if isinstance(res.exception, DbtUsageException):
        exit_with_error(str(res.exception))
    else:
        pass

    # match res.exception:
    #     case DbtUsageException():
    #         exit_with_error(str(res.exception))
    #     case _:
    #         pass

    return 0 if res.success else 1


# Register the custom initialization command with the original dbt CLI.
dbt_main.cli.add_command(init_dbtmu)

if __name__ == "__main__":
    sys.exit(cli())

import subprocess
import typer
from cli.utils import remove_all_package_builds, root_env_vars, build_all_packages

app = typer.Typer()


@app.command()
def indexer():
    remove_all_package_builds()
    build_all_packages()
    parse()
    indexer_tests = run_indexer_tests()
    if indexer_tests.returncode != 0:
        raise typer.Exit(code=1)


@app.command()
def parse():
    parse_test = run_parse_tests()
    print(parse_test.returncode)
    if parse_test.returncode != 0:
        raise typer.Exit(code=1)


def run_parse_tests():
    print("Running parser tests...")
    subprocess.run(
        "docker compose -f packages/parse/docker-compose.tests.yaml down --remove-orphans",
        shell=True,
    )
    return subprocess.run(
        "docker compose -f packages/parse/docker-compose.tests.yaml up --build --abort-on-container-exit",
        shell=True,
    )


def run_indexer_tests():
    print("Running indexer tests...")
    subprocess.run(
        "docker compose -f packages/indexer/docker-compose.tests.yaml down --remove-orphans",
        shell=True,
    )
    return subprocess.run(
        "docker compose -f packages/indexer/docker-compose.tests.yaml up --build --abort-on-container-exit",
        shell=True,
    )

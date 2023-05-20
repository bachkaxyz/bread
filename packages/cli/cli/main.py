import subprocess
import typer

app = typer.Typer()

test = typer.Typer()


@test.command()
def indexer():
    remove_all_package_builds()
    build_all_packages()
    load_env_vars()
    parse()
    indexer_tests = run_indexer_tests()
    if indexer_tests.returncode != 0:
        raise typer.Exit(code=1)


@test.command()
def parse():
    parse_test = run_parse_tests()
    print(parse_test.returncode)
    if parse_test.returncode != 0:
        raise typer.Exit(code=1)


def remove_all_package_builds():
    pass


def load_env_vars():
    subprocess.run("bash scripts/load_env_vars.sh", shell=True)


def build_all_packages():
    print("Building all packages...")
    subprocess.run("bash poetry/build.sh", shell=True)


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


app.add_typer(test, name="test")

if __name__ == "__main__":
    app()

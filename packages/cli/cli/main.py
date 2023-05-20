import subprocess
import typer

app = typer.Typer()

test = typer.Typer()


@test.command()
def indexer():
    remove_all_package_builds()
    build_all_packages()
    load_env_vars()
    run_parse_tests()
    run_indexer_tests()


@test.command()
def parse():
    run_parse_tests()


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
    test = subprocess.run(
        "docker compose -f packages/parse/docker-compose.tests.yaml up --build",
        shell=True,
    )
    if test.returncode != 0:
        print("Parser tests failed!")
        exit(1)


def run_indexer_tests():
    print("Running indexer tests...")
    subprocess.run(
        "docker compose -f packages/indexer/docker-compose.tests.yaml down --remove-orphans",
        shell=True,
    )
    test = subprocess.run(
        "docker compose -f packages/indexer/docker-compose.tests.yaml up --build --abort-on-container-exit",
        shell=True,
    )
    if test.returncode != 0:
        print("Parser tests failed!")
        exit(1)


app.add_typer(test, name="test")

if __name__ == "__main__":
    app()

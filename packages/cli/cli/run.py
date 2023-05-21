import subprocess
from cli.utils import build_all_packages, remove_all_package_builds, root_env_vars
import typer

app = typer.Typer()


@app.command()
def indexer(redeploy: bool = True, prod=False):
    env = root_env_vars()
    if redeploy:
        stop_indexer(prod=prod, env=env)
        remove_all_package_builds()
    build_all_packages()
    start_indexer(prod=prod, env=env)


def start_indexer(prod: bool = False, env: dict = {}):
    if prod:
        subprocess.run(
            f"docker compose -f packages/indexer/docker-compose.yaml up --build --abort-on-container-exit",
            shell=True,
            env=env,
        )
    else:
        subprocess.run(
            f"docker compose -f packages/indexer/docker-compose.yaml -f packages/indexer/docker-compose.local.yaml up --build --abort-on-container-exit",
            shell=True,
            env=env,
        )


def stop_indexer(prod: bool = False, env: dict = {}):
    subprocess.run(
        f"docker compose -f packages/indexer/docker-compose.yaml down",
        shell=True,
        env=env,
    )

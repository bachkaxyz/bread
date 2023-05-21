import subprocess
from typing import List
from cli.utils import build_all_packages, remove_all_package_builds, root_env_vars
import typer
from python_on_whales.docker_client import DockerClient
from python_on_whales.utils import ValidPath

app = typer.Typer()


@app.command()
def indexer(redeploy: bool = True, prod: bool = False):
    compose_files: List[ValidPath] = ["packages/indexer/docker-compose.yaml"]
    env = root_env_vars()
    if not prod:
        compose_files.append("packages/indexer/docker-compose.local.yaml")
    docker = DockerClient(
        compose_files=compose_files,
        compose_env_file=".env",
        compose_project_name=env["COMPOSE_PREFIX"] + "-indexer"
        if env["COMPOSE_PREFIX"]
        else "",
    )

    if redeploy:
        docker.compose.down(remove_orphans=True)
        remove_all_package_builds()
    build_all_packages()

    try:
        docker.compose.up(detach=True, build=True)
    except:
        raise typer.Exit(code=1)


@app.command()
def dagster(redeploy: bool = True, prod: bool = False):
    compose_files: List[ValidPath] = []
    if prod:
        compose_files.append("packages/dagster/docker-compose.prod.yaml")
    else:
        compose_files.append("packages/dagster/docker-compose.local.yaml")

    env = root_env_vars()
    docker = DockerClient(
        compose_files=compose_files,
        compose_env_file=".env",
        compose_project_name=env["COMPOSE_PREFIX"] + "-dagster"
        if env["COMPOSE_PREFIX"]
        else "",
    )

    if redeploy:
        docker.compose.down(remove_orphans=True)
        remove_all_package_builds()
    build_all_packages()
    try:
        docker.compose.up(detach=True, build=True)
    except:
        raise typer.Exit(code=1)


@app.command()
def api(prod: bool = False):
    compose_files: List[ValidPath] = ["packages/api/docker-compose.yaml"]
    if not prod:
        compose_files.append("packages/api/docker-compose.local.yaml")
    env = root_env_vars()
    docker = DockerClient(
        compose_files=compose_files,
        compose_env_file=".env",
        compose_project_name=env["COMPOSE_PREFIX"] + "-api"
        if env["COMPOSE_PREFIX"]
        else "",
    )
    docker.compose.down(remove_orphans=True)
    remove_all_package_builds()
    build_all_packages()
    try:
        docker.compose.up(
            detach=True,
            build=True,
        )
    except:
        raise typer.Exit(code=1)


@app.command()
def dashboard(prod=False):
    compose_files: List[ValidPath] = []
    if not prod:
        compose_files.append("packages/dashboard/docker-compose.local.yaml")
    compose_files.append("packages/dashboard/docker-compose.yaml")
    env = root_env_vars()
    docker = DockerClient(
        compose_files=compose_files,
        compose_env_file=".env",
        compose_project_name=env["COMPOSE_PREFIX"] + "-dashboard"
        if env["COMPOSE_PREFIX"]
        else "",
    )
    docker.compose.down(remove_orphans=True)
    remove_all_package_builds()
    build_all_packages()
    try:
        docker.compose.up(detach=True, build=True)
    except:
        raise typer.Exit(code=1)

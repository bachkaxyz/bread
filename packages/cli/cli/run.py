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
    if not prod:
        compose_files.append("packages/indexer/docker-compose.local.yaml")
    docker = DockerClient(compose_files=compose_files, compose_env_file=".env")

    if redeploy:
        docker.compose.down(remove_orphans=True)
        remove_all_package_builds()
    build_all_packages()

    docker.compose.up(detach=True, build=True)


@app.command()
def dagster(redeploy: bool = True, prod: bool = False):
    compose_files: List[ValidPath] = ["packages/dagster/docker-compose.yaml"]

import os
import subprocess
from typing import List, Optional
from cli.utils import build_all_packages, remove_all_package_builds, root_env_vars
import typer
from python_on_whales.docker_client import DockerClient
from python_on_whales.utils import ValidPath

app = typer.Typer()


@app.command()
def indexer(
    redeploy: bool = True,
    prod: bool = False,
    build_docker: bool = True,
    build_packages: bool = True,
    open_shell: bool = False,
    command: Optional[str] = typer.Argument(None, help="Command to run in the shell"),
):
    compose_files: List[ValidPath] = ["packages/indexer/docker-compose.yaml"]
    env = root_env_vars()
    if not prod:
        compose_files.append("packages/indexer/docker-compose.local.yaml")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        env["GOOGLE_APPLICATION_CREDENTIALS"]
        if env["GOOGLE_APPLICATION_CREDENTIALS"]
        else ""
    )
    print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    docker = DockerClient(
        compose_files=compose_files,
        compose_env_file=".env",
        compose_project_name=env["COMPOSE_PREFIX"] + "-indexer"
        if env["COMPOSE_PREFIX"]
        else "",
    )

    if redeploy:
        docker.compose.down(remove_orphans=True)
    if build_packages:
        remove_all_package_builds()
        build_all_packages()

    try:
        if open_shell:
            if build_docker:
                docker.compose.build(
                    ["indexer"],
                )
            docker.compose.run("indexer", command=["bash"], tty=True, remove=True)
        else:
            args = {"detach": True}
            if command:
                args["command"] = command
            if build_docker:
                args["build"] = True

            docker.compose.up(**args)
    except:
        raise typer.Exit(code=1)


@app.command()
def dagster(redeploy: bool = True, prod: bool = False):
    compose_files: List[ValidPath] = []
    if prod:
        compose_files.append("packages/dags/docker-compose.prod.yaml")
    else:
        compose_files.append("packages/dags/docker-compose.local.yaml")

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
def dashboard(prod: bool = False):
    compose_files: List[ValidPath] = []
    compose_files.append("packages/dashboard/docker-compose.yaml")
    if not prod:
        compose_files.append("packages/dashboard/docker-compose.local.yaml")
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

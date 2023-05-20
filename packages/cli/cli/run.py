from cli.utils import build_all_packages, load_env_vars, remove_all_package_builds
import typer

app = typer.Typer()


@app.command()
def indexer(redeploy: bool = True, prod=False):
    if redeploy:
        stop_indexer()
        remove_all_package_builds()
    load_env_vars()
    build_all_packages()
    start_indexer()


def start_indexer():
    pass


def stop_indexer():
    pass

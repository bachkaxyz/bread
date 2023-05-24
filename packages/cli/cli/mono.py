from collections import OrderedDict
from enum import Enum
import subprocess
import sys
import typer
from typing_extensions import Annotated

app = typer.Typer()


class Package(Enum):
    CLI = "./packages/cli"
    DAGS = "./packages/dags"
    DASHBOARD = "./packages/dashboard"
    API = "./packages/api"
    INDEXER = "./packages/indexer"
    PARSE = "./packages/parse"
    ROOT = "."


# this graph shows the packages that each package depends on
# so Package.Parse is used in Package.Indexer and Package.Root
# so anytime we update Package.Parse, we need to update Package.Indexer and Package.Root
dep_graph = {
    Package.CLI: [Package.ROOT],
    Package.API: [Package.ROOT],
    Package.DAGS: [Package.ROOT],
    Package.DASHBOARD: [Package.ROOT],
    Package.INDEXER: [Package.ROOT],
    Package.PARSE: [Package.INDEXER, Package.ROOT],
    Package.ROOT: [],
}


def get_reliant_packages(package: Package, packages_to_update=None):
    if packages_to_update is None:
        packages_to_update = OrderedDict()
    packages_to_update[package] = None
    for dependent in dep_graph[package]:
        get_reliant_packages(dependent, packages_to_update)
    return packages_to_update


@app.command()
def add_dep(package: Annotated[Package, typer.Option(prompt=True)], dep: str):
    # when we add a new dependency, we need to update the lockfile for that package, and all of its dependents, and the root
    # this does all of that

    packages_to_update = get_reliant_packages(package)
    # first add dep to the package
    subprocess.run(
        f"poetry add {dep}",
        shell=True,
        cwd=package.value,
    )

    for p in packages_to_update:
        subprocess.run(f"poetry lock", shell=True, cwd=p.value)


@app.command()
def rm_dep(package: Annotated[Package, typer.Option(prompt=True)], dep: str):
    # when we remove a dependency, remove package, and remove from all of its dependents, and the root

    reliant_packages = get_reliant_packages(package)

    subprocess.run(
        f"poetry remove {dep}",
        shell=True,
        cwd=package.value,
    )

    for p in reliant_packages:
        subprocess.run(f"poetry lock", shell=True, cwd=p.value)

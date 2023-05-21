import subprocess
from cli.utils import build_all_packages, load_env_vars
import typer
from cli import test, run

app = typer.Typer()
app.add_typer(test.app, name="test")
app.add_typer(run.app, name="run")


@app.command()
def build():
    build_all_packages()


@app.command()
def load_vars():
    load_env_vars()


if __name__ == "__main__":
    app()

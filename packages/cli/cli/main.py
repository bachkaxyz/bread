import subprocess
import typer
from cli import test, run

app = typer.Typer()
app.add_typer(test.app, name="test")
app.add_typer(run.app, name="run")

if __name__ == "__main__":
    app()

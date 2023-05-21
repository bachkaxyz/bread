import os
import subprocess
from typing import Dict

from dotenv import dotenv_values


def remove_all_package_builds():
    print("Removing all package builds...")
    subprocess.run("bash poetry/remove_builds.sh", shell=True)


def root_env_vars() -> Dict[str, str | None]:
    print(f"Loading root env vars in {os.getcwd()}")
    return dotenv_values(".env")


def build_all_packages():
    subprocess.run("bash poetry/build.sh", shell=True)

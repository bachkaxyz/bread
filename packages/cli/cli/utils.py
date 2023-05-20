import subprocess


def remove_all_package_builds():
    subprocess.run("bash scripts/remove_all_package_builds.sh", shell=True)


def load_env_vars():
    subprocess.run("bash scripts/_load_root_env_vars.sh", shell=True)


def build_all_packages():
    print("Building all packages...")
    subprocess.run("bash poetry/build.sh", shell=True)

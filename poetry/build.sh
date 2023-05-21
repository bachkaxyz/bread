#!/bin/sh
# This script builds all the poetry packages, creating wheels, dists, and requirements.txt's
# All the wheels will be placed in both the root folder's dist, and in a dist folder within each package
set -u
set -e
DIR="$( cd "$( dirname "$0" )" && pwd )"
cd "${DIR}/.." || exit

VERSION=$(poetry version | awk '{print $2}')
echo "Building version: $VERSION"
if [ "$(uname)" = "Darwin" ]; then export SEP=" "; else SEP=""; fi

# all python packages, in topological order
. ${DIR}/projects.sh
_projects=$PROJECTS
echo "Running on following projects: ${_projects}"
for p in $_projects
do
  cd "${DIR}/../packages/${p}" || exit
  # change path deps in project def
  sed -i$SEP'' "s|{.*path.*|\"^$VERSION\"|" pyproject.toml
  # include project changelog
  poetry build
  # export deps, with updated path deps
  mkdir -p info
  poetry export -f requirements.txt --output ./info/requirements.txt --without-hashes --with-credentials
  sed -i$SEP'' "s/ @ .*;/==$VERSION;/" "./info/requirements.txt"
done

# -u for update
if [ "$(uname)" = "Darwin" ]; then export FLAG=" "; else FLAG="-u "; fi
echo "=========="
mkdir -p "${DIR}/../info"
# cp $FLAG "${DIR}/../VERSION" "${DIR}/../info/"
echo "=========="
# copying each wheel to root folder dist
mkdir -p "${DIR}/../dist"
for p in $_projects
do
  # ls -altr "${DIR}/../packages/${p}/dist/"
  cp $FLAG "${DIR}/../packages/${p}/dist/"*".whl" "${DIR}/../dist/"
done
echo "=========="
# then copying these to each project
for p in $_projects
do
  cp $FLAG "${DIR}/../dist/"*".whl" "${DIR}/../packages/${p}/dist/"
#   cp $FLAG "${DIR}/../info/"*"" "${DIR}/../packages/${p}/info/"
done

# this script chnages the path dependencies in the pyproject.toml files to the current version
# we need to reset them to the original state
for p in $_projects
do
  cd "${DIR}/../packages/${p}" || exit
  git checkout HEAD pyproject.toml
done
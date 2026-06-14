#!/usr/bin/bash

ECHO="/usr/bin/echo"
RM="/usr/bin/rm"
UV="/home/ubuntu/.local/bin/uv"

${ECHO}
${ECHO} "Updating uv Python environment...."

# Nuke uv download cache
${ECHO} "  - Clearing users's uv download cache at ~/.cache/uv/"
${RM} -rf ~/.cache/uv

# Nuke virtual env
${ECHO} "  - Removing Python virtual environment if there is one"
${RM} -rf ./.venv

# Update uv to latest Python 3.14
${ECHO} "  - Upgrading uv Python to latest 3.14 patch version"
${UV} python upgrade --quiet 3.14

# Update pyproject.toml to latest version of all Python deps
${ECHO} "  - Updating pyproject.toml with latest released versions of all dependencies"
${UV} lock --upgrade --quiet

# Install newly-refreshed list of dep versions
${ECHO} "  - Installing refreshed list of Python package dependencies"
${UV} sync --quiet

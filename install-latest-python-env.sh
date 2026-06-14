#!/usr/bin/bash

ECHO="/usr/bin/echo"
RM="/usr/bin/rm"
UV="/usr/local/bin/uv"

${ECHO}
${ECHO} "Updating uv Python environment...."

# Nuke uv download cache
${ECHO} "  - Clearing users's uv download cache at ~/.cache/uv/"
${RM} -rf ~/.cache/uv

# Nuke virtual env
${ECHO} "  - Removing Python virtual environment if there is one"
${RM} -rf ./.venv

# Update uv to latest Python interpreter that's >= 3.14.0
${ECHO} "  - Installing/upgrading uv Python to latest stable >= 3.14.0"
${UV} python upgrade --quiet ">=3.14"

# Update pyproject.toml to latest version of all Python deps
${ECHO} "  - Updating pyproject.toml with latest stable versions of all dependencies"
${UV} lock --upgrade --quiet

# Install newly-refreshed list of dep versions
${ECHO} "  - Installing all dependencies in the newly-refreshed pyproject.toml"
${UV} sync --quiet

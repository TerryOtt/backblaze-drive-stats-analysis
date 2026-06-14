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
${ECHO} "  - Removing Python virtual environment if there is one (\"./.venv/\")"
${RM} -rf ./.venv

# Remove all managed (i.e., non-managed) uv Python installs
${ECHO} "  - Uninstalling all uv-managed Python interpreters"
${UV} python uninstall --all --quiet

# Update uv to latest Python interpreter that's >= 3.14.0
${ECHO} "  - Installing uv-managed Python interpreter: latest stable >= 3.14.0"
${UV} python install --quiet ">=3.14"

# Update uv.lock with latest version of all Python deps
${ECHO} "  - Updating uv.lock with latest releases of all dependencies"
${UV} lock --upgrade --quiet

# Install newly-refreshed list of dep versions in uv.lock
${ECHO} "  - Installing all dependencies in the newly-refreshed uv.lock"
${UV} sync --quiet

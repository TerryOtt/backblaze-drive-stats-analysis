#!/usr/bin/bash

RM="/usr/bin/rm"
UV="/home/ubuntu/.local/bin/uv"

# Nuke uv download cache
${RM} -rf ~/.cache/uv

# Nuke virtual env
${RM} -rf ./.venv

# Update uv to latest Python
${UV} python upgrade

# Update to latest version of all Python deps
${UV} sync --upgrade

# Install latest versions of all deps
${UV} sync

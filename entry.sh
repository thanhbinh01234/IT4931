#!/bin/bash
__conda_setup="$(/opt/conda/bin/conda shell.bash hook 2> /dev/null)"
eval "$__conda_setup"
conda activate "${CONDA_ENV:-base}"
exec "$@" 2>&1
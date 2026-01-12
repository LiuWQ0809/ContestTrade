#!/usr/bin/env bash
set -e

# ContestTrade Simplified Starter
ENV_NAME="contesttrade"

# Find Conda
CONDA_PATH=$(command -v conda || true)
if [[ -z "$CONDA_PATH" ]]; then
    for loc in "$HOME/anaconda3" "$HOME/miniconda3" "/opt/anaconda3" "/opt/miniconda3" "/home/liuwq/miniconda3"; do
        if [[ -f "$loc/etc/profile.d/conda.sh" ]]; then
            CONDA_PATH="$loc/bin/conda"
            break
        fi
    done
fi

if [[ -z "$CONDA_PATH" ]]; then
    echo "Error: Conda not found. Please install Miniconda/Anaconda."
    exit 1
fi

# Activate Conda
CONDA_BASE=$(dirname "$(dirname "$CONDA_PATH")")
source "$CONDA_BASE/etc/profile.d/conda.sh"

if ! conda info --envs | grep -qE "^$ENV_NAME\s"; then
    echo "Creating environment '$ENV_NAME'..."
    conda create -y -n "$ENV_NAME" python=3.10
    conda activate "$ENV_NAME"
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    echo "Activating environment '$ENV_NAME'..."
    conda activate "$ENV_NAME"
fi

# Run Program
echo "Starting ContestTrade..."
python auto_trade/main.py

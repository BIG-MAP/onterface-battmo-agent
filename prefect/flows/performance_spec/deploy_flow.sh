#!/bin/bash

# First check if the required number of arguments are passed.
if [ $# -ne 2 ]; then
    echo "Usage: $0 <python_file> <function>"
    exit 1
fi

# Assign the arguments to variables
PYTHON_FILE=$1
FUNCTION=$2

# Define the commands to be run
BUILD_COMMAND="//home/jovyan/.local/bin/prefect deployment build -n performance_opt -p default-agent-pool -sb local-file-system/local -q test $PYTHON_FILE:$FUNCTION"
DEPLOY_COMMAND="/home/jovyan/.local/bin/prefect deployment apply $FUNCTION-deployment.yaml"
RUN_COMMAND="/home/jovyan/.local/bin/prefect agent start -p 'default-agent-pool'"

# Run the commands
echo "Running build command"
$BUILD_COMMAND

echo "Running deploy command"
$DEPLOY_COMMAND

echo "Running run command"
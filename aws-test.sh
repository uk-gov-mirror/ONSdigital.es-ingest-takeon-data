#!/bin/bash

# set -e

# Run flake8 linting
flake8 GIT-repository 

# && echo "flake8 passed. Yayyy." || exit 1

# Run unit tests
pytest GIT-repository
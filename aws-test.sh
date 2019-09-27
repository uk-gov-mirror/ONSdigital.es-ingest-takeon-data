#!/bin/bash

# set -e

cd GIT-repository

# Run flake8 linting
flake8

# && echo "flake8 passed. Yayyy." || exit 1

# Run unit tests
pytest
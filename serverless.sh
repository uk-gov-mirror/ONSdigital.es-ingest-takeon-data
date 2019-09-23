#!/usr/bin/env bash

cd GIT-repository
echo Packaging serverless bundle...
serverless package --package pkg
find ./pkg
serverless deploy --verbose;
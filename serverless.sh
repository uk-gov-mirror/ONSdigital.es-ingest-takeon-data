#!/usr/bin/env bash
cd subnets
cat security_groups_output.json

cd deploy-repository

echo Packaging serverless bundle...

serverless package --package pkg
serverless deploy --verbose;
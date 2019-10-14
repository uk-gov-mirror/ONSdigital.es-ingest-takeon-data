#!/usr/bin/env bash
cd subnets
ls
cat security_groups_output.json

cd ../
cd deploy-repository

echo Packaging serverless bundle...

serverless package --package pkg
serverless deploy --verbose;
#!/usr/bin/env bash
cd subnets
cat subnets_output.json

cd ../
ls
pwd
cd deploy-repository

echo Packaging serverless bundle...

serverless package --package pkg
serverless deploy --verbose;
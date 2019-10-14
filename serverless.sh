#!/usr/bin/env bash
cd subnets
ls
cat subnets.json
cd ../
cd deploy-repository

echo Packaging serverless bundle...
serverless package --package pkg
serverless deploy --verbose;
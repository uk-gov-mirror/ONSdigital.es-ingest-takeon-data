#!/usr/bin/env bash

cd deploy-repository

echo Packaging serverless bundle...
serverless package --package pkg
mv pkg ../
cd ../
ls
ls subnets
serverless deploy --verbose;
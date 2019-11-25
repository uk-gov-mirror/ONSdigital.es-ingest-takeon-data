#!/usr/bin/env bash

cd ingest-deploy-repository
echo Destroying serverless bundle...
serverless remove --verbose;
#!/usr/bin/env bash

cd ingest-repository
echo Destroying serverless bundle...
serverless remove --verbose;

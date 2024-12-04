#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres

# Set the password to dbos, default user is postgres.
docker run -d --network host --rm --name="jack" --env POSTGRES_PASSWORD=Test@123 postgres:latest
#!/bin/bash

# Linux or macOS bash script to execute the export process.
# Chris Joakim, Microsoft

mkdir -p app/exports
mkdir -p app/tmp

rm app/exports/*.*
rm app/tmp/*.*

gradle checkEnv

gradle exportGremlinViaSqlEndpoint

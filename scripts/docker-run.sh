#!/bin/bash

mkdir -p databases
docker build -t manul-tracer .
docker run -p 8501:8501 -v $(pwd)/databases:/app/databases manul-tracer
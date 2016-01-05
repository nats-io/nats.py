#!/bin/bash

export PYTHONPATH=$(pwd)

python tests/client_test.py
python tests/parser_test.py

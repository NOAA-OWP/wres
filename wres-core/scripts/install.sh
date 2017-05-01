#!/bin/bash

if [ $# -lt 3 ]; then
    echo ''
    echo 'usage: ./install.sh <database username> <database host> <database name>'
    echo ''
else
    scripts/compile
    cd SQL
    ./BuildDatabase $1 $2 $3
fi

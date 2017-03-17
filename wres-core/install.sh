#!/bin/bash

if [ $# -lt 2 ]; then
    echo ''
    echo 'usage: ./install.sh <database username> <database name>'
    echo ''
else
    ant
    cd SQL
    ./BuildDatabase $1 $2
fi

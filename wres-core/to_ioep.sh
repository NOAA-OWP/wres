#!/bin/bash

if [ $# -eq 0 ]; then
    echo 'usage: ./to_ioep.sh <IOEP Username>'
else
  ant
  scp wres-core.jar $1@NWCAL-WRF-HydroIOCEval:/home/share/WRES/
fi

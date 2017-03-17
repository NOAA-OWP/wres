#!/bin/bash

if [ $# -eq 0 ]; then
    echo 'usage: ./to_ioep.sh <IOEP Username>'
else
  ant
  scp wres saveObservation saveForecast wres-core.jar $1@NWCAL-WRF-HydroIOCEval:/home/share/WRES/
  scp SQL/* $1@NWCAL-WRF-HydroIOCEval:/home/share/WRES/SQL/
  scp resources/* $1@NWCAL-WRF-HydroIOCEval:/home/share/WRES/resources/
fi

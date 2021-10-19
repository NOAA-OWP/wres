# Water Resources Evaluation Service

This document is for power users of WRES: those who wish to run WRES locally
rather than use the WRES NWCAL instance HTTP API service.

## Prerequisites

1. Java 11 runtime environment
2. Access to Postgres 10+ database instance for larger-than-RAM deployments, or
   alternatively RAM sufficient to hold the datasets for one evaluation.

## Set up the database

Skip this step if not using postgresql.

To use postgresql, ask the software development team to help with this.

## Run the software

Ensure that the database you wish to connect to is specified in this file:

        lib/conf/wresconfig.xml

For in-RAM (no postgresql) use the example H2 string found in this document.

### Unix systems

        bin/wres execute /path/to/project.xml

### Windows systems

        bin\wres.bat execute c:/path/to/project.xml

This will run an evaluation project configuration specified in "project.xml"
from start to finish.

You may run the wres script from anywhere, and the project file may be in any
location or any file name, absolute or relative.

### Optional

Standard java parameters you wish to pass in for troubleshooting can be set in
JAVA_OPTS. Additionally, a system property named wres.logLevel will set the
amount of logging that occurs during a run. As an example, to increase verbosity
to the debug level:

#### Unix

        JAVA_OPTS="-Dwres.logLevel=debug" bin/wres execute project.xml

#### Windows

        set JAVA_OPTS="-Dwres.logLevel=debug" && bin\wres.bat execute project.xml

### Viewing log files

Messages will be printed to standard out during execution and also written
with more detail to a directory inside your user home directory. By default,
the total size of this directory will be trimmed daily to a couple hundred megs.

Also by default, the log file name includes the date.

#### Unix

        cat $HOME/wres_logs/wres.[YYYY]-[MM]-[DD].log

#### Windows

        type %USERPROFILE%\wres_logs\wres.[YYYY]-[MM]-[DD].log

## Set up an evaluation project configuration document

See [Project Configuration](projectconfig.html)

## Advanced changes to system behavior (unnecessary for operation)

See [Advanced Alterations of System Behavior](advanced.html)

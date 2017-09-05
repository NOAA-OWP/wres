# Water Resources Evaluation Service

## Prerequisites

1. Java 8 runtime environment
2. Access to Postgres 9.6 database system

## Setting up the database

For the time being, ask the software development team to help with this.

## Running the software

Ensure that the database you wish to connect to is specified in this file:

        lib/conf/wresconfig.xml

### Unix systems

        bin/wres execute /path/to/project.xml

### Windows systems

        bin\wres.bat execute c:/path/to/project.xml

This will run a project specified in "project.xml" from start to finish.

You may run the wres script from anywhere, and the project file may be in any
location or any file name, absolute or relative.

### Optional

Standard java parameters you wish to pass in for troubleshooting can be set in
JAVA_OPTS. Additionally, a system property named logLevel will set the amount
of logging that occurs during a run. As an example, to increase verbosity to
the debug level:

#### Unix

        JAVA_OPTS="-DlogLevel=debug" bin/wres execute project.xml

#### Windows

        set JAVA_OPTS="-DlogLevel=debug" && bin\wres.bat execute project.xml

### Viewing log files

Messages will be printed to standard out during execution and also written
with more detail to a directory inside your user home directory. By default,
the total size of this directory will be trimmed daily to a couple hundred megs.

#### Unix

        cat $HOME/wres_logs/wres.log

#### Windows

        type %USERPROFILE%\wres_logs\wres.log

## Setting up a project

See [Project Configuration](projectconfig.html)

## Advanced changes to system behavior (unnecessary for operation)

See [Advanced Alterations of System Behavior](advanced.html)

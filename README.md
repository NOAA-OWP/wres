# Water Resources Evaluation Service

Also known as "WRES" (said "Water RES" aloud)

This is the top-level directory of WRES, which is a "control" module in itself
of the WRES software.

Several submodules are also present here.

To build all of WRES for production distribution, run

    ./gradlew check javadoc distZip

The resulting zip file should be found in build/distributions directory. If
there was a clean git status, there should be a zip file that is named something
like "wres-20180418-2177338.zip". If there was not a clean git status, there
should be a zip file that is named something more like
"wres-20180418-2177338-dev.zip" which indicates this software should not be
sent through the delivery pipeline.

To build all of WRES for local use, run

    ./gradlew check javadoc installDist

This is similar to unzipping the production distribution zip locally. The wres
software will be present in build/install/wres directory, as if unzipped.

To run an evaluation with WRES, run

    bin/wres execute project_config.xml

See doc/index.html for more information (either inside build/install/wres or
inside an unzipped version of WRES).

# Water Resources Evaluation Service (WRES)

The Water Resource Evaluation Service, also known as WRES, is a comprehensive service for evaluating the quality of existing 
and emerging NWC and RFC models and forecast systems.

To build WRES for local use, run the following commands in your prefered terminal

    ./gradlew check javadoc installDist

This is similar to unzipping the production distribution zip locally. The wres
software will be present in build/install/wres directory, as if unzipped.

Change directory to the unzipped project location to execute projects
    
    cd build/install/wres/

To execute a project you can run the following command:

    bin/wres execute yourProject.yml

## Example Evaluation
Running the following commands will execute a test project from the executable you have created
(This is running successfully is reliant on being in the 'wres/build/install/wres' directory)

    bin/wres execute ../../../systests/testScenario/evaluation.yml

## Running Against Last Release

* Navigate to the releases page:
https://github.com/NOAA-OWP/wres/releases

* Download the latest core zip from the assets of the most recent deploy
  * Should look like wres-DATE-VERSION.zip
* Unzip the directory and navigate into the folder like above


    cd build/install/wres/

* Execute your project


    bin/wres execute yourProject.yml


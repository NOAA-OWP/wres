# Water Resources Evaluation Service (WRES)

The Water Resource Evaluation Service (WRES) is a software tool for evaluating the quality of existing and emerging hydrometeorological models
and forecasting systems. It may be deployed as a standalone executable or as a web service (with a backend cluster of executables).

To build WRES for standalone use, run the following commands in your preferred terminal:

    ./gradlew check javadoc installDist

This will produce a zip distribution in the build/install/wres directory of your machine. Alternatively, download the zip distribution corresponding to a
publicly released version of the WRES (see below for instructions). Unzip the distribution and navigate into the top-level directory. To run the WRES, you
will need a recent version of the Java Runtime Environment (JRE) installed. To check whether you have an appropriate JRE installed locally, you can examine
the result of the following command:

    java -version

If this reports a version greater than 17.0, you can execute the WRES. Otherwise, you will need to install an appropriate JRE.

To execute an evaluation, you can run the following command on a Linux-like operating system:

    bin/wres myEvaluation.yml

On a Windows-like operating system, you can execute the following command:

    bin/wres.bat myEvaluation.yml

Where myEvaluation.yml is the file that declares your evaluation.

## Example Evaluation
* Create a file `predictions.csv` with the following content:

```
value_date,variable_name,location,measurement_unit,value
1985-06-01T13:00:00Z,streamflow,myLocation,CMS,21.0
1985-06-01T14:00:00Z,streamflow,myLocation,CMS,22.0
```

* Create a file `observation.csv` with the following content:

```
value_date,variable_name,location,measurement_unit,value
1985-06-01T13:00:00Z,streamflow,myLocation,CMS,23.0
1985-06-01T14:00:00Z,streamflow,myLocation,CMS,25.0
```

* Create a file `myEvaluation.yml` with the following content, adjusting the paths to reference the files you created (if you created the files inside the bin directory, no changes are needed):

```
observed: observations.csv
predicted: predictions.csv
```

* Execute the evaluation:


    bin/wres myEvaluation.yml

By default, the results of the evaluation will be written to the user's temporary directory. The paths to the files should be reported on the console. For example:

`Wrote 2 paths to foo.user/temp/wres_evaluation_7woOxSGA-AEvyg3eNSS_j9Jj9Hc`

## Running Against the Last Release

* Navigate to the releases page:
  https://github.com/NOAA-OWP/wres/releases
* Download the latest core zip from the assets of the most recent deploy
  * Should look like `wres-DATE-VERSION.zip`
* Unzip the directory and navigate into the folder like above
```  
cd build/install/wres/
```

* Execute your project
```
bin/wres myEvaluation.yml
```
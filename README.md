![commitChecks](https://github.com/NOAA-OWP/wres/actions/workflows/commitChecks.yml/badge.svg) [![DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/NOAA-OWP/wres)

# Water Resources Evaluation Service (WRES)

The Water Resources Evaluation Service (WRES) is a comprehensive service for evaluating the quality of model predictions, such as hydrometeorological forecasts. The WRES encapsulates a data-to-statistics evaluation pipeline, including reading data from web services or files of various formats, rescaling data, changing measurement units, filtering data, pairing predictions and observations, allocating pairs to pools based on pooling criteria (e.g., common forecast lead times), computing statistics and writing statistics to various formats. It can support different shapes and sizes of evaluations, ranging from a few geographic features (e.g., using an in-memory data model for quick performance), or evaluations containing many thousands of geographic features (e.g., using a database, such as PostgreSQL) or even gridded datasets.

As described in the [wiki](https://github.com/NOAA-OWP/wres/wiki#what-are-the-options-for-deploying-and-operating-wres), there are three modes of operation for the WRES: (1) "cluster mode" using a web-service instance; (2) "standalone mode" using a short-running instance; and (3) "standalone mode" using a long-running, local-server instance. 

This WRES is broadly composed of the following software:

* **The core WRES software.** The core WRES is the software that performs an evaluation. It parses an evaluation declaration, acquires and pairs the data, creates the statistics and writes the statistics to formats. It can be executed as a stand-alone, from the command line, but is typically executed by users through a web service front-end.
* **The WRES web service layer.** The service software wraps the core WRES inside of a web-service deployment. The service layer includes the following modules: the `wres-tasker`, which distributes evaluation tasks, the `wres-worker`, which works evaluation tasks, the `wres-redis`, which persists evaluation information, and the `wres-broker`, which allows the various components to communicate. The relationship between the `wres-tasker` and the `wres-worker` is 1: many per deployment.

While the web-service layer is only necessary for a cluster deployment, the core WRES is required for all modes of operation. 

## How can I obtain the software?

The WRES software is obtained by cloning this repository. No other repository is needed. 

## How can I build the software?

Gradle is used to build the software and create the release artifacts. Once the repository is cloned, to build and unit test the software, run this command from the root directory of your clone:

    ./gradlew build

To install the WRES locally for standalone use, including generated Javadoc, run the following command:

    ./gradlew check javadoc installDist

This will produce a release distribution in the `./build/install/wres` directory relative to the root directory of your repository. 

## How can I run the WRES using a standalone?

To run the WRES, you will need the Java Runtime Environment (JRE) 17.0 or later installed. To check whether you have an appropriate JRE installed locally, you can examine the result of the following command:

    java -version

If this reports a version greater than 17.0, you can execute the WRES. Otherwise, you will need to install an appropriate JRE.

To execute an evaluation, you can run the following command on a Linux-like operating system from within the `./build/install/wres` directory:

    `bin/wres myEvaluation.yml`

On a Windows-like operating system, you can execute the following command:

    `bin/wres.bat myEvaluation.yml`

Where `myEvaluation.yml` is the file that declares your evaluation; see the [Declaration language](https://github.com/NOAA-OWP/wres/wiki/Declaration-Language) wiki for more information.

## How can I create a simple example evaluation?

Do the following within the `./build/install/wres` directory: 

1. Create a file `predictions.csv` with the following content:

```
value_date,variable_name,location,measurement_unit,value
1985-06-01T13:00:00Z,streamflow,myLocation,CMS,21.0
1985-06-01T14:00:00Z,streamflow,myLocation,CMS,22.0
```

2. Create a file `observations.csv` with the following content:

```
value_date,variable_name,location,measurement_unit,value
1985-06-01T13:00:00Z,streamflow,myLocation,CMS,23.0
1985-06-01T14:00:00Z,streamflow,myLocation,CMS,25.0
```

3. Create a file `myEvaluation.yml` with the following content, adjusting the paths to reference the files you created (if you created the files inside the bin directory, no changes are needed):

```
observed: observations.csv
predicted: predictions.csv
```

4. Execute the evaluation as follows:

    `bin/wres myEvaluation.yml`

By default, the results of the evaluation will be written to the user's temporary directory. The paths to the files should be reported on the console. For example:

`Wrote 2 paths to foo.user/temp/wres_evaluation_7woOxSGA-AEvyg3eNSS_j9Jj9Hc`

## Running Against the Last Release

To run against the latest official release of the WRES, do the following: 

1. Navigate to the latest release: https://github.com/NOAA-OWP/wres/releases/latest.

2. Download the core .zip from the assets for that latest release. The file to obtain should follow this naming convention: `wres-DATE-VERSION.zip`

3. Unzip the core .zip and navigate into the folder that is created. For example:

```  
cd wres-*
```

4. Execute your project

```
bin/wres myEvaluation.yml
```
## Where can I find more information?

For user documentation, see the [wiki](https://github.com/NOAA-OWP/wres/wiki).

For more information intended for developers, see the [Instructions for Developers](https://github.com/NOAA-OWP/wres/wiki/Instructions-for-Developers) wiki. 

----

## Licensing

1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)

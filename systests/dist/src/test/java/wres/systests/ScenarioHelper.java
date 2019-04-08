package wres.systests;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import wres.Main;
import wres.control.Control;

/**
 * A class to be used to when setting up system test scenarios of the WRES.
 *
 * The class makes optional use of environment variables to identify the system
 * tests directory (which will typically be the working directory for
 * executions), and WRES database information.
 *
 * It then passes through environment variables to  already-unset Java system
 * properties before running the WRES.
 * @author Raymond.Chui
 * @author Hank.Herr
 * @author jesse.bickel
 */

public class ScenarioHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ScenarioHelper.class );

    static final String USUAL_EVALUATION_FILE_NAME = "project_config.xml";

    private ScenarioHelper()
    {
        // Static utility helper class, disallow construction.
    }

    /**
     * Sets the properties which drive the system testing.  These use system environment variables in order
     * to set Java system properties.
     */
    static void setAllPropertiesFromEnvVars( Scenario scenarioInfo )
    {
		LOGGER.info( "####>> Setting properties for run based on user settings..." );
        //The databae host.
        String dbHostFromEnvVar = System.getenv( "WRES_DB_HOSTNAME" );
        String dbHostFromSysProp = System.getProperty( "wres.url" );
        if ( dbHostFromSysProp == null && dbHostFromEnvVar != null
             && !dbHostFromEnvVar.isEmpty() )
        {
            System.setProperty( "wres.url", dbHostFromEnvVar );
        }

        //The database name.
        String dbNameFromEnvVar = System.getenv( "WRES_DB_NAME" );
        String dbNameFromSysProp = System.getProperty( "wres.databaseName" );
        if ( dbNameFromSysProp == null && dbNameFromEnvVar != null
             && !dbNameFromEnvVar.isEmpty() )
        {
            System.setProperty( "wres.databaseName", dbNameFromEnvVar );
        }

        //The database user.
        String dbUserFromEnvVar = System.getenv( "WRES_DB_USERNAME" );
        String dbUserFromSysProp = System.getProperty( "wres.username" );
        if ( dbUserFromSysProp == null && dbUserFromEnvVar != null
             && !dbUserFromEnvVar.isEmpty() )
        {
            System.setProperty( "wres.username", dbUserFromEnvVar );
        }

        // Passphrase should be acquired from postgres passphrase file. -Jesse
        // Do not set the password through this mechansim. 

        // I thinks it's too late to attempt to set log level here. -Jesse
        //TODO Test the impact of setting the log level on the output from a test.  If it does
        //not inpact the log output, then something is wrong.  See what happens if you set
        //wres.logLevel here to the WRES_LOG_LEVEL env var and then passing that through via 
        //the execute script below.  Remove this TODO once the log level can be set correctly.

        //Set he temp directory.
        /* System.setProperty( "java.io.tmpdir",
                            scenarioInfo.getScenarioDirectory()
                                        .toString() ); */
		// By Redmine ticket 51654#387, item 2, set the tmpdir to ~/....../systests/outputs
		String testOutputs = Paths.get(System.getProperty("user.dir")).toFile().getParentFile().getAbsolutePath() + "/outputs";
		File testOutputsDir = Paths.get(testOutputs).toFile();
		if (testOutputsDir.isFile()) {
			testOutputsDir.delete();
			testOutputsDir.mkdir();
		} else if (! testOutputsDir.exists()) {
			testOutputsDir.mkdir();
		} else if (testOutputsDir.isDirectory()) {
			// we don't delete old output directories
			// Because we want to save the output directories after tests.
			// Let the testing script clean the old output directories
			; 
		}	
		System.setProperty("java.io.tmpdir", testOutputs);
        LOGGER.info( "Properties used to run test:" );
        LOGGER.info( "    wres.hostname = " + System.getProperty( "wres.hostname" ) );
        LOGGER.info( "    wres.url = " + System.getProperty( "wres.url" ) );
        LOGGER.info( "    wres.databaseName = " + System.getProperty( "wres.databaseName" ) );
        LOGGER.info( "    wres.username = " + System.getProperty( "wres.username" ) );
        LOGGER.info( "    wres.logLevel =  " + System.getProperty( "wres.logLevel" ) );
        LOGGER.info( "    wres.password =  " + System.getProperty( "wres.password" ) );
        LOGGER.info( "    user.dir (working directory) =  " + System.getProperty( "user.dir" ) );
    }


    /**
     * Single entry point for executing the scenario.  Modify this to call the desired private method below.
     * It should be a direct pass through and the method called should confirm that execution was successful.
     * @param scenarioInfo The {@link Scenario} information.
     */
    //static void assertExecuteScenario( Scenario scenarioInfo )
    public static Control assertExecuteScenario( Scenario scenarioInfo )
    {
		LOGGER.info( "####>> Beginning test execution... " + scenarioInfo.getName());
        return assertExecuteScenarioThroughControl( scenarioInfo ); //If this is used, return its returned Control.
        //assertExecuteScenarioThroughControl( scenarioInfo ); //If this is used, return its returned Control.
//        assertExecuteScenarioThroughProcessBuilder( scenarioInfo );
//        assertExecuteScenarioThroughMainWithShutdownHook(scenarioInfo);
    }


    /**
     * Executes the system test through a call to {@link Control}.  Note that using this call requires that
     * the Gradle build.gradle set forkEvery=1.  Otherwise, there is something left over after {@link Control}
     * is used that causes a second test, 003, to fail when run after the first test, 001.
     * @param scenarioInfo The {@link Scenario} information.
     * @return The {@link Control} used to execute the test.  Calling {@link Control#get()} can return the output
     * paths which may be useful.
     */
    private static Control assertExecuteScenarioThroughControl( Scenario scenarioInfo )
    {
        Path config = scenarioInfo.getScenarioDirectory().resolve( ScenarioHelper.USUAL_EVALUATION_FILE_NAME );
        String args[] = { config.toString() };
        Control wresEvaluation = new Control();
System.out.println("java.io.tmpdir ================ " + System.getProperty("java.io.tmpdir"));
        int exitCode = wresEvaluation.apply( args );
        assertEquals( "Execution of WRES failed with exit code " + exitCode
                      + "; see log for more information!",
                      0,
                      exitCode );
        return wresEvaluation;
    }

    /**
     * This only works when the compare against benchmarks is included in the shutdown hook.  That is because
     * the {@link Main#main(String[])} has a System exit within it that kills the test.  We could break out the 
     * innards of main to separate it from System.exit and call that instead.  However, that would result in 
     * the same problem as seen in the {@link Control} version above unless every test is run in its own
     * JVM; that is 003 fails after 001.
     * @param scenarioInfo Scenario information.
     */
    private static void assertExecuteScenarioThroughMainWithShutdownHook( Scenario scenarioInfo )
    {
        Path config = scenarioInfo.getScenarioDirectory()
                                  .resolve( ScenarioHelper.USUAL_EVALUATION_FILE_NAME );
        Runtime.getRuntime().addShutdownHook( new Thread( () -> 
        {
            LOGGER.info( "####>> Asserting that output matches benchmarks." );
            ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo );
        } ) );
        String args[] = { "execute", config.toString() };
        Main.main( args );
    }

    /**
     * Executes the system test in a separate JVM through the use of {@link ProcessBuilder}.  This will fail the unit test
     * if an exception occurs running the process or the exit value for WRES is something other than 0.  This currently
     * does not work due to a {@link LinkageError} that occurs when initializing a static variable within {@link Main}.
     * @param scenarioInfo The {@link Scenario} information.
     */
    private static void assertExecuteScenarioThroughProcessBuilder( Scenario scenarioInfo )
    {
        //For example: https://www.pixelstech.net/article/1461746551-Launch-Java-process-programmatically-in-Java
        String javaHome = System.getProperty( "java.home" );
        String javaBin = javaHome + File.separator
                         + "bin"
                         + File.separator
                         + "java";
        //Does lib/conf need to be on path???  If so Gradle may need to set that correctly.
        String classpath = System.getProperty( "java.class.path" ); 
        String className = "wres.Main";

        //Get the path to the configuration.
        Path config = scenarioInfo.getScenarioDirectory()
                                  .resolve( ScenarioHelper.USUAL_EVALUATION_FILE_NAME );

// XXX Note that I never got this to work as is.  It would always complain about a LinkageError involving initializing static vars.
        ProcessBuilder builder = new ProcessBuilder(
                                                     javaBin,
                                                     "-Djava.util.logging.config.class=wres.system.logging.JavaUtilLoggingRedirector",
                                                     "-Djava.io.tmpdir='" + scenarioInfo.getScenarioDirectory().toString() + "'",
                                                     "-Dwres.url='" + System.getProperty( "wres.url" ) + "'",
                                                     "-Dwres.hostname='" + System.getProperty( "wres.url" ) + "'",
                                                     "-Dwres.databaseName='" + System.getProperty( "wres.databaseName" )+ "'",
                                                     "-Dwres.username='" + System.getProperty( "wres.username" ) + "'",
                                                     "-Duser.dir='" + System.getProperty( "user.dir" ) + "'",
                                                     "-cp", 
                                                     classpath,
                                                     className,
                                                     "execute",
                                                     config.toString() );

//XXX INFORMATION DISCOVERED DURING EXPERIMENTS.
// If I were to use the wres script technique, I could pass in a system property via Gradle pointing to the unzipped release dir in order to find bin/wres.
// Otherwise, it would have no clean way to find it (it would need to search the dist/build directory, which would be ugly)/
//             ProcessBuilder builder = new ProcessBuilder("/home/hank.herr/wresTestScriptWorkingRelease/bin/wres", "execute", config.toString());
//
//These are the Java options used in the wres script.  After adding the logging stuff above, I don't see anything missing.
//
//        builder.environment().put( "DEFAULT_JVM_OPTS", "\"-Xms1628m\" \"-Xmx1628m\" \"-XX:+HeapDumpOnOutOfMemoryError\" \"-Djava.util.logging.config.class=wres.system.logging.JavaUtilLoggingRedirector\"" );
//        builder.environment().put( "JAVA_OPTS", "-XX:+HeapDumpOnOutOfMemoryError -Xms128m -Xmx3072m -Dwres.logLevel=info -Dwres.url=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.hostname=***REMOVED***wresdb-dev01.***REMOVED***.***REMOVED*** -Dwres.databaseName=wres6 -Dwres.username=wres_user6" );
//        builder.environment().put( "JAVA_OPTS",
//                                   "-Dwres.url='" + System.getProperty( "wres.url" ) + "'" 
//                                   + " -Dwres.databaseName='" + System.getProperty( "wres.databaseName" ) + "'" 
//                                   + " -Dwres.username='" + System.getProperty( "wres.username" ) + "'" );
//
//James believed that the class path was the issue.  I never confirmed.

        //Results in a version of the command executed that can be selected and executed.  
        //This always works even when the ProcessBuilder call fails.  Why does this work when the above didn't?
        String commandStr = "";
        for ( String item : builder.command() )
        {
            commandStr += item + " ";
        }
        LOGGER.info( "####>> COMMAND EXECUTE = " + commandStr + "\n\n\n" );

        //Redirect stderr to stdout and then send all screen output to a file within the scenario directory.
        //TODO Create a better output file name, perhaps based on the wres_eval output directory name?
        //Or the current date/time stamp?
        builder.redirectErrorStream( true ); //Merges error with output stream
        builder.redirectOutput( new File( scenarioInfo.getScenarioDirectory().toFile(),
                                          "test." + scenarioInfo.getName() + ".screenout.txt" ) );

        //Start the process.  Wait until it is done.
        Process process;
        try
        {
            LOGGER.debug( "Starting the system test process for scenario " + scenarioInfo.getName() );
            process = builder.start();
            process.waitFor();
            LOGGER.debug( "Completed system test process for " + scenarioInfo.getName() );
            int exitValue = process.exitValue();
            assertEquals( "WRES execution resulted in non-zero exit code, " + exitValue, 0, exitValue );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "Unexpected IOException occurred starting the ProcessBuilder: " + e.getMessage() );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
            fail( "InterruptedException occurred waiting for the system test execution to complete: "
                  + e.getMessage() );
        }
    }

    /**
    * Delete wres_evaluation_output_* from previous run.
    * @param directoryToLookIn The directory in which to look for evaluation output subdirectories.
    * @return True if anything is deleted, false otherwise.
    * @throws IOException
    */
    public static void deleteOldOutputDirectories( Path directoryToLookIn )
    {
        File directoryWithFiles = directoryToLookIn.toFile();

        if ( !directoryWithFiles.exists() || !directoryWithFiles.canRead()
             || !directoryWithFiles.isDirectory() )
        {
            throw new IllegalArgumentException( "Could not read a directory at "
                                                + directoryToLookIn );
        }

        String[] files = directoryToLookIn.toFile().list();

        //Search the files for anything that appears to be wres evaluation output and remove the entire directory.
        try
        {
            for ( int i = 0; i < files.length; i++ )
            {
                if ( files[i].startsWith( "wres_evaluation_output" ) ) //TODO Is there a constant somewhere that stores this???
                {
                    Path outputPath = directoryToLookIn.resolve( files[i] );
                    System.out.println( "Deleting old system testing output directory, "
                                        + outputPath.toFile().getAbsolutePath() );
					LOGGER.info( "####>> Deleting old system testing output directory, "
                                        + outputPath.toFile().getAbsolutePath() );
                    FileUtils.deleteDirectory( outputPath.toFile() );
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "Problem encountered removed old output directories: " + e.getMessage() );
        }
    }

    /**
     * Checks for output validity from WRES and fails if not.  This is used in conjunction
     * with {@link #assertOutputsMatchBenchmarks(Scenario, Control)}.
     */
    static void assertWRESOutputValid( Control completedEvaluation )
    {
        //Obtain the complete list of outputs generated.
        Set<Path> initialOutputSet = completedEvaluation.get();

        //Confirm all outputs were written to the same directory.
        if ( !initialOutputSet.isEmpty() )
        {
            Iterator<Path> paths = initialOutputSet.iterator();
            Path firstPath = paths.next();
            while ( paths.hasNext() )
            {
                Path secondPath = paths.next();
                if ( !firstPath.getParent().equals( secondPath.getParent() ) )
                {
                    fail( "Not all outputs of WRES Control.apply were written to the same directory.  "
                          + "That is not allowed in system testing." );
                }
            }
        }

        //Anything else to validate about the output?
    }

    /**
     * Asserts if the output path was found for the given scenario.  It fails if no acceptable output path is found or if 
     * more than one is found.
     * @param scenarioInfo Test scenario information.
     * @return The path to the output folder.
     */
    private static Path assertOutputPathValidAndReturnIt( Scenario scenarioInfo )
    {
        //Find the output folder.
        File folder = scenarioInfo.getScenarioDirectory().toFile();
        File[] outputFolders = folder.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                return name.startsWith( "wres_evaluation_output_" );
            }
        } );

        //There should be exactly one if this method is called.
        if ( outputFolders.length == 0 )
        {
            fail( "No output folder found." );
        }
        if ( outputFolders.length > 1 )
        {
            fail( "Multiple output folders found." );
        }
        return outputFolders[0].toPath();
    }

    /**
     * List the files contained within the output folder for the scenario.
     * @param scenarioInfo Scenario information.
     * @return Set specifying the output paths.
     */
    private static Set<Path> obtainOutputPathsForScenario( Scenario scenarioInfo )
    {
        Path outputFolderPath = assertOutputPathValidAndReturnIt( scenarioInfo );

        //Log the output folder name.
        LOGGER.info( "Found output folder, " + outputFolderPath );

        //List the files.
        File[] outputFiles = outputFolderPath.toFile().listFiles();

		// I want to check each file
		for (int i = 0; i < outputFiles.length; i++)
		{
			System.out.println("Output file " + i + outputFiles[i].getName());
		}

        //Covert to paths and return.  There may be a utility for this; ask.
        //Set should ensure one instance of each path.
        Set<Path> outputPaths = new HashSet<Path>();
        for ( File file : outputFiles )
        {
			if (file.getName().endsWith(".csv")) // only need *.csv files
			{
				System.out.println("Add this file " + file.getName());
            	outputPaths.add( file.toPath() );
			}
			else
			{
				System.out.println("This isn't s CSV file " + file.getName());
			}
        }
        return outputPaths;
    }

    /**
     * Assertion method for checking outputs against benchmarks that works from the directory, itself, instead 
     * of {@link Control}.  This will work with any version of {@link #assertExecuteScenario(Scenario)}, which
     * is why its what is used now. However, if we finalize the decision to use {@link Control}, then we may want
     * to call {@link #assertOutputsMatchBenchmarks(Scenario, Control)} instead of this.<br>
     * <br>
     * This will fail out if the {@link #assertOutputPathValidAndReturnIt(Scenario)}, called via 
     * {@link #obtainOutputPathsForScenario(Scenario)} fails (number of output dirs is not 1) or if
     * {@link #compareOutputAgainstBenchmarks(Scenario, Set)} returns a non-zero code.
     */
    static void assertOutputsMatchBenchmarks( Scenario scenarioInfo )
    {
        //Get a set of the output paths.  There is an assertion method within the call confirming output
        //was generated.
        Set<Path> initialOutputSet = obtainOutputPathsForScenario( scenarioInfo );
        LOGGER.info( "Found " + initialOutputSet.size() + " files.  Comparing against benchmarks." );

        try
        {
            //Construct the directory listing file for the output set.  Add the dir listing
            //to the final set of outputs.
            //
            //Redmine 51654#387 decided not to compare the dirListing.txt
            //Path dirListingPath = constructDirListingFile( initialOutputSet );
            //HashSet<Path> finalOutputSet = Sets.newHashSet( initialOutputSet );
			HashSet<Path> finalOutputSet = new HashSet<Path>();
            //finalOutputSet.add( dirListingPath );

            // Need to filter out the *.png and the *.nc files
			for ( Path nextPath : initialOutputSet )
            {
                if ( nextPath.endsWith( ".png" ) || nextPath.endsWith( ".nc" ) )
                {
                    LOGGER.info( "Won't add this path ===== {}", nextPath );
                }
                else
                {
                    LOGGER.info( "Will add this path ===== {}", nextPath );
                    finalOutputSet.add( nextPath );
                }
            }
            //Call the compare method to obtain a result code and check that it is zero.
            int resultCode = compareOutputAgainstBenchmarks( scenarioInfo,
                                                         finalOutputSet );
            assertEquals( "Camparison with benchmarks failed with code " + resultCode + ".", 0, resultCode );
        }
        catch ( IllegalStateException e )
        {
            e.printStackTrace();
            fail( "Problem encountered removed old output directories: " + e.getMessage() );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "IOException encountered removed old output directories: " + e.getMessage() );
        }
    }


    /**
     * This builds the dirListing.txt file for that directory and then compares all of the
     * outputs.  Anything in the output directory that has a corresponding benchmark will be diffed.  If anything
     * in the benchmarks is not found in the outputs, then a difference is reported; this should be equivalent to   
     * check dirListing.txt, in that something in the benchmarks but not in the output should result in a dirListing.txt   
     * difference, but I wanted to be certain nothing fell through the cracks.  <br>   
     * <br>    
     * If exceptions are thrown when calling this method, it indicates something basic went wrong that is not    
     * covered by the result code return.  Typically, that will be due to a system test setup poorly for    
     * any one of various reasons, so it excepts out.    
     * @return The comparison result code.    
     * @throws IllegalStateException Indicates that the outputs were written to different directories.  Its expected that all    
     * system test output is written to the same directory.    
     * @throws IOException If the directory listing file cannot be generated or the files cannot be compared.   
     */
    static void assertOutputsMatchBenchmarks( Scenario scenarioInfo,
                                              Control completedEvaluation )
    {
		LOGGER.info( "####>> Assert outputs match benchmarks..." + scenarioInfo.getName() );
        //Assert the output as being valid and then get the output from the provided Control if so.
        assertWRESOutputValid( completedEvaluation );
		/*
		// List files one-by-one
		Set<Path> tmpset = completedEvaluation.get();
		Path tmppath = tmpset.iterator().next();
		File tmpdir = tmppath.getParent().toFile();
		Set<Path> initialOutputSet = new HashSet<Path>();
		if (tmpdir.isDirectory() && tmpdir.canRead() && tmpdir.canExecute())
		{
			File[] tmpFiles = tmpdir.listFiles();
			//Set<Path> initialOutputSet = new HashSet<Path>();
			for (int i = 0; i < tmpFiles.length; i++)
			{
				System.out.println("tmpFile = " + tmpFiles[i].toString());
				initialOutputSet.add(tmpFiles[i].toPath());
			}
		}
		else
        	initialOutputSet = completedEvaluation.get(); // somehow this Control.get() couldn't complete get all files for scerio1000 and 1001
		*/

        Set<Path> initialOutputSet = completedEvaluation.get();

        //Create the directory listing.
        //Path dirListingPath;
        try
        {	
			//Redmine 51654#387 decided not to compare the dirListing.txt
            //dirListingPath = constructDirListingFile( initialOutputSet );
            // Below Sets.newHasSet() won't filter out the *.png and *.nc files
            //HashSet<Path> finalOutputSet = Sets.newHashSet( initialOutputSet );
			HashSet<Path> finalOutputSet = new HashSet<Path>();
            // Need to filter out the *.png and the *.nc files
            for ( Path nextPath : initialOutputSet )
            {
                if ( nextPath.endsWith( ".png" ) || nextPath.endsWith( ".nc" ) )
                {
                    LOGGER.info( "Won't add this path ===== {}", nextPath );
                }
                else
                {
                    LOGGER.info( "Will add this path ===== {}", nextPath );
                    finalOutputSet.add( nextPath );
                }
            }
			// do not check the dirListing for now, see Redmine 51654#387
            //finalOutputSet.add( dirListingPath );

            int resultCode = compareOutputAgainstBenchmarks( scenarioInfo,
                                                         finalOutputSet );
            assertEquals( "Camparison with benchmarks failed with code " + resultCode + ".", 0, resultCode );
        }
        catch ( IllegalStateException e )
        {
            e.printStackTrace();
            fail( "Problem encountered removed old output directories: " + e.getMessage() );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "IOException encountered removed old output directories: " + e.getMessage() );
        }
    }


    /**
     * Constructs the dirListing.txt file for the outputs generated.
     * @param generatedOutputs It is assumed that all outputs are generated in the same directory.
     * @return The {@link Path} to the directory listing file created.
     * @throws IOException
     */
    private static Path constructDirListingFile( Set<Path> generatedOutputs )
            throws IOException
    {

        //Gather the list of file names.
        List<String> sortedOutputsGenerated = new ArrayList<>();
        Path outputParentPath = null;
        for ( Path path : generatedOutputs )
        {
            sortedOutputsGenerated.add( path.getFileName().toFile().getName() );
            if ( outputParentPath == null )
            {
                outputParentPath = path.getParent();
            }
        }

        //TODO This does not use the same sort as a linux directory listing.  Why?  I want it to
        //be the same so that I don't need to redo benchmark dirListing.txt files.
        //Sort.
        Collections.sort( sortedOutputsGenerated );

        //Define the dir listing file name in t he output parent directory.
        Path dirListingPath = Paths.get( outputParentPath.toString(), "dirListing.txt" );
        Files.write( dirListingPath, sortedOutputsGenerated );
        return dirListingPath;
    }

    /**
     * Compare the evaluation *.csv files with benchmarks
     * @param evaluationPath -- evaluation directory path
     * @return 0 no errors; 2 output not found; 4 sort failed; 8 found diff in txt files; 16 found diff in sorted_pairs; 32 found diff in csv files
     */
    private static int compareOutputAgainstBenchmarks( Scenario scenarioInfo,
                                                       Set<Path> generatedOutputs )
            throws IOException, IllegalStateException
    {
        //Establish the benchmarks directory and obtain a listing of all benchmarked files.
        Path benchmarksPath = scenarioInfo.getScenarioDirectory()
                                          .resolve( "benchmarks" );
        File benchmarksDir = benchmarksPath.toFile();
        List<String> benchmarkedFiles = new ArrayList<>();
        if ( benchmarksDir.exists() && benchmarksDir.isDirectory() )
        {
            benchmarkedFiles.addAll( Arrays.asList( benchmarksDir.list() ) );
			if (benchmarkedFiles.contains("dirListing.txt"))
				benchmarkedFiles.remove("dirListing.txt"); // for now we don't compare this file
        }
        else
        {
            throw new IllegalStateException( "The benchmark directory, " + benchmarksPath.toString()
                                             + " is not defined or is not a directory; "
                                             + "that is not allowed in system testing." );
        }

        //For every output file...
        int pairResultCode = 0;
        int metricCSVResultCode = 0;
        int txtResultCode = 0;
        int miscResultCode = 0;
        for ( Path outputFilePath : generatedOutputs )
        {
			
            String outputFileName = outputFilePath.toFile().getName();
			LOGGER.info("output file name = " + outputFileName);
			
            //For the pairs, you need to sort them first.
            File benchmarkFile = identifyBenchmarkFile( outputFilePath, benchmarksPath );
            if ( benchmarkFile != null )
            {
                try
                {
                    //Pairs has its own method because it has to sort the lines.
                    if ( outputFileName.endsWith( "pairs.csv" ) )
                    {
                        assertOutputPairsEqualExpectedPairs( outputFilePath.toFile(), benchmarkFile );
                    }
                    //Otherwise just do the comparison without sorting.
                    else
                    {
                        assertOutputTextFileMatchesExpected( outputFilePath.toFile(), benchmarkFile );
                    }
                }
                //Modify the result code if an assertion error is reported.
                catch ( AssertionError e )
                {
                    if ( outputFileName.endsWith( "pairs.csv" ) )
                    {
                        pairResultCode = 16;
						LOGGER.warn("The pair result code " + pairResultCode + " with file name " + outputFileName);
                    }
                    //Otherwise just do the comparison without sorting.
                    else if ( outputFileName.endsWith( ".csv" ) )
                    {
                        metricCSVResultCode = 32;
						LOGGER.warn("The metric CSV result code " + metricCSVResultCode + " with file name " + outputFileName);
                    }
                    else if ( outputFileName.endsWith( ".txt" ) )
                    {
                        txtResultCode = 4;
						LOGGER.warn("The text result code " + txtResultCode + " with file name " + outputFileName);
                    }
                    else
                    {
                        miscResultCode = 2;
						LOGGER.warn("The miscellaneous result code " + miscResultCode + " with file name " + outputFileName);
                    }
                }
                //Remove the benchmark as one to check.
                benchmarkedFiles.remove( benchmarkFile.getName() );
            }
        }
        if ( !benchmarkedFiles.isEmpty() )
        {
            LOGGER.warn( "The following benchmarked files were not present in the evaluation output directory: "
                                + Arrays.toString( benchmarkedFiles.toArray() ) );
            miscResultCode = 2;
        }

        return pairResultCode + metricCSVResultCode + txtResultCode + miscResultCode;
    }


    /**
     *
     * @param outputFilePath Output file for which to identify the benchmark.
     * @param benchmarkDirPath The directory for benchmarks.
     * @return The file identified or null if none if no appropriate file is found.
     */
    private static File identifyBenchmarkFile( Path outputFilePath, Path benchmarkDirPath )
    {
        File benchmarkFile = null;
        File outputFile = outputFilePath.toFile();
        if ( outputFile.getName().endsWith( "pairs.csv" ) )
        {
            benchmarkFile = Paths.get( benchmarkDirPath.toString(), outputFile.getName() ).toFile();
            if ( !benchmarkFile.isFile() || !benchmarkFile.canRead() )
            {
                //First attempt wasn't good; look for sorted_*.
                benchmarkFile = Paths.get( benchmarkDirPath.toString(), "sorted_" + outputFile.getName() ).toFile();
                if ( !benchmarkFile.isFile() || !benchmarkFile.canRead() )
                {
                    return null;
                }
            }
        }
        //Otherwise just do the comparison.
        else
        {
            benchmarkFile = Paths.get( benchmarkDirPath.toString(), outputFile.getName() ).toFile();
            if ( !benchmarkFile.isFile() || !benchmarkFile.canRead() )
            {
                return null;
            }
        }
        return benchmarkFile;
    }

    /**
     * Asserts that the provided file matches that found in the benchmarks, if one exists.  This method will
     * only work with text files.  However, as long as binary files are benchmarked, you can pass binary files
     * into this method and it won't do anything with them because the benchmark won't exist.
     * @param outputFile
     * @param benchmarkFile
     * @throws IOException
     */
    private static void assertOutputTextFileMatchesExpected( File outputFile, File benchmarkFile ) throws IOException
    {
        //Ensure that the output is a readable file.  The benchmark file has already been established as such.
        assertTrue( outputFile.isFile() && outputFile.canRead() );

        //Read in all of the data.  May need a lot of memory!
        List<String> actualRows = Files.readAllLines( outputFile.toPath() );
        List<String> expectedRows = Files.readAllLines( benchmarkFile.toPath() );
        //Files must not be zero sized and must be identical in number of lines.
        assertTrue( actualRows.size() > 0 && expectedRows.size() > 0 );
        assertEquals( actualRows.size(), expectedRows.size() );

        // Verify by row, rather than all at once
        for ( int i = 0; i < actualRows.size(); i++ )
        {
            //TODO Added trimming below to handle white space at the ends, but should I?
            //Mainly worried about the Window's carriage return popping up some day.
			LOGGER.info("compare output file " + outputFile.getName() + " with benchmarks file " + benchmarkFile.getName());
			LOGGER.info("Are they equals ? " + actualRows.get( i ).equals(expectedRows.get( i )));
			int expectedRowsIndex = expectedRows.indexOf(actualRows.get( i ));
            assertEquals( "For output file, " + outputFile.getName()
                          + ", row "
                          + i
                          + " differs from benchmark.",
                          actualRows.get( i ).trim(), expectedRows.get( expectedRowsIndex ).trim() );
                          //actualRows.get( i ), expectedRows.get( expectedRowsIndex ) );
        }
    }

    /**
     * Asserts that the output pairs are equal to a benchmark file, if one exists.
     * @param pairsFile
     * @param benchmarkDirPath
     * @throws IOException
     */
    private static void assertOutputPairsEqualExpectedPairs( File pairsFile, File benchmarkFile ) throws IOException
    {
        //Ensure that the output is a readable file.  The benchmark file has already been established as such.
        assertTrue( pairsFile.isFile() && pairsFile.canRead() );

        //Read in all of the data.  May need a lot of memory!
        List<String> actualRows = Files.readAllLines( pairsFile.toPath() );
        List<String> expectedRows = Files.readAllLines( benchmarkFile.toPath() );

        //Files must not be zero sized and must be identical in number of lines.
        assertTrue( actualRows.size() > 0 && expectedRows.size() > 0 );
        assertEquals( actualRows.size(), expectedRows.size() );

        // Sort in natural order
        Collections.sort( actualRows );
        Collections.sort( expectedRows );

        // Verify by row, rather than all at once
        for ( int i = 0; i < actualRows.size(); i++ )
        {
            assertEquals( "For pairs file file, " + pairsFile.getName()
                          + ", after sorting alphabetically, row "
                          + i
                          + " differs from benchmark.",
                          actualRows.get( i ).trim(), //TODO Added trimming to handle white space at the ends, but should I?
                          expectedRows.get( i ).trim() );
        }
    }

    //===================================================================================================================
    //TODO Everything below has not been changed, because I have not implemented 501 yet.  These are tools Raymond
    //developed to assist with the before and after script execution.
    //===================================================================================================================

    /**
    * if there is a after script, do it now
    * @param files -- a list of files
    * @return -- false if there is no after script; Otherwise, true
    */
    static boolean doAfter( String[] files )
    {
        boolean isAnAfterScript = false;
        for ( int i = 0; i < files.length; i++ )
        {
            if ( files[i].endsWith( "after.sh" ) )
            {
                isAnAfterScript = true;
                LOGGER.info( "Found " + files[i] );
                searchAndReplace( System.getProperty( "user.dir" ) + "/" + files[i] );
            }
        }
        return isAnAfterScript;
    }

    /**
    * if there is a before script, do it 1st
    * @param files -- a list of files
    * @return -- false if there is no before script; Otherwise, true
    */
    static boolean doBefore( String[] files )
    {
        boolean isABeforeScript = false;
        for ( int i = 0; i < files.length; i++ )
        {
            if ( files[i].endsWith( "before.sh" ) )
            {
                isABeforeScript = true;
                LOGGER.info( "Found " + files[i] );
                searchAndReplace( System.getProperty( "user.dir" ) + "/" + files[i] );
            }
        }
        return isABeforeScript;
    }

    /**
    * Search and replace a string in a file
    * @param fileName -- file to read and write
    * @param searchFor -- a string to search for
    * @param replace -- a string to replace
    * @param line -- a specify line number search/replace, or 'g' for global
    */
    static void searchAndReplace( String fileName, String searchFor, String replace, String line )
    {
        File file = Paths.get( System.getProperty( "user.dir" ) + "/" + fileName ).toFile();
        
        if ( file.exists() )
        {
            try
            {
                ArrayList<String> arrayList = new ArrayList<String>();
                BufferedReader bufferedReader = new BufferedReader( new FileReader( file ) );
                String aLine;
                int lineNumber = 0;
                while ( ( aLine = bufferedReader.readLine() ) != null )
                {
                    if ( aLine.indexOf( searchFor ) >= 0 )
                    {
                        aLine = aLine.replaceAll( searchFor, replace );
                        LOGGER.info( "Replaced line " + lineNumber + " to " + aLine );
                    }
                    arrayList.add( aLine );
                    lineNumber++;
                }
                bufferedReader.close();
                lineNumber = 0;
                BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter( file ) );
                for ( Iterator<String> iterator = arrayList.iterator(); iterator.hasNext(); lineNumber++ )
                {
                    bufferedWriter.write( iterator.next().toString() );
                    bufferedWriter.newLine();
                }
                bufferedWriter.flush();
                bufferedWriter.close();
            }
            catch ( FileNotFoundException fnfe )
            {
                System.err.println( fnfe.getMessage() );
            }
            catch ( IOException ioe )
            {
                System.err.print( ioe.getMessage() );
            }
        }
        else
        {
            System.err.println( "File " + file.getPath() + " doesn't existed." );
        }

    }

    /**
    * Search for token File=, Search=, Replace=, and Line= from a file
    * @param fileName -- a before.sh or after.sh shell script
    */
    static void searchAndReplace( String fileName )
    {
        File file = Paths.get( fileName ).toFile();
        if ( file.exists() )
        {
            try
            {
                BufferedReader bufferedReader = new BufferedReader( new FileReader( file ) );
                String aLine;
                String[] theFile = null;
                String[] search = null;
                String[] replace = null;
                String[] line = null;
                while ( ( aLine = bufferedReader.readLine() ) != null )
                {
                    int index = 0;
                    if ( ( index = aLine.lastIndexOf( "File=" ) ) > 0 )
                    {
                        theFile = aLine.substring( index ).split( "=" );
                    }
                    else if ( ( index = aLine.lastIndexOf( "Search=" ) ) > 0 )
                    {
                        search = aLine.substring( index ).split( "=" );
                    }
                    else if ( ( index = aLine.lastIndexOf( "Replace=" ) ) > 0 )
                    {
                        replace = aLine.substring( index ).split( "=" );
                    }
                    else if ( ( index = aLine.lastIndexOf( "Line=" ) ) > 0 )
                    {
                        line = aLine.substring( index ).split( "=" );
                    }
                } // end while loop
                bufferedReader.close();
                searchAndReplace( theFile[1].trim(), search[1].trim(), replace[1].trim(), line[1].trim() );
            }
            catch ( FileNotFoundException fnfe )
            {
                System.err.println( fnfe.getMessage() );
            }
            catch ( IOException ioe )
            {
                System.err.print( ioe.getMessage() );
            }
        }
        else
        {
            System.err.println( "File " + file.getPath() + " doesn't existed." );
        }
    } // end method

    /**
     * Return the directory where system test project configs and data live.
     */
    static Path getBaseDirectory()
    {
        String baseDirectoryFromEnvVar = System.getenv( "TESTS_DIR" );

        if ( baseDirectoryFromEnvVar == null
             || baseDirectoryFromEnvVar.isEmpty() )
        {
            throw new IllegalStateException(
                                             "Expected an environment variable TESTS_DIR" );
        }

        return Paths.get( baseDirectoryFromEnvVar );
    }

} // end this class

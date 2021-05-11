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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.ExecutionResult;
import wres.control.Control;
import wres.io.concurrency.Executor;
import wres.io.utilities.Database;
import wres.system.SystemSettings;

/**
 * A class to be used when setting up system test scenarios of the WRES for 
 * JUnit execution.
 *
 * The class makes optional use of environment variables to identify the system
 * tests directory and WRES database information.
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

    private static final String USUAL_EVALUATION_FILE_NAME = "project_config.xml";
    private static final SystemSettings SYSTEM_SETTINGS = SystemSettings.fromDefaultClasspathXmlFile();
    private static final Database DATABASE = new Database( SYSTEM_SETTINGS );
    private static final Executor EXECUTOR = new Executor( SYSTEM_SETTINGS );

    private ScenarioHelper()
    {
        // Static utility helper class, disallow construction.
    }

    /**
     * Sets the properties which drive the system testing.  These use system environment variables in order
     * to set Java system properties.
     * @param@ scenarioInfo The {@link ScenarioInformation} information.
     */
    static void logUsedSystemProperties( ScenarioInformation scenarioInfo )
    {
        LOGGER.info( "Properties used to run test:" );
        LOGGER.info( "    wres.url = " + System.getProperty( "wres.url" ) + " (the database host name)");
        LOGGER.info( "    wres.databaseName = " + System.getProperty( "wres.databaseName" ) );
        LOGGER.info( "    wres.username = " + System.getProperty( "wres.username" ) );
        LOGGER.info( "    wres.logLevel =  " + System.getProperty( "wres.logLevel" ) );
        LOGGER.info( "    wres.password =  " + System.getProperty( "wres.password" ) + " (its recommended to use the .pgpass file to identify the database password)");
        LOGGER.info( "    wres.dataDirectory =  " + System.getProperty( "wres.dataDirectory" ) );
        LOGGER.info( "    user.dir (working directory) =  " + System.getProperty( "user.dir" ) );
        LOGGER.info( "    java.io.tmpdir =  " + System.getProperty( "java.io.tmpdir" ) );
    }


    /**
     * Single entry point for executing the scenario.  Modify this to call the desired private method below.
     * It should be a direct pass through and the method called should confirm that execution was successful.
     * @param scenarioInfo The {@link ScenarioInformation} information.
     */
    protected static Set<Path> executeScenario( ScenarioInformation scenarioInfo )
    {
        LOGGER.info( "Beginning test execution through JUnit for scenario: " + scenarioInfo.getName());
        Path config = scenarioInfo.getScenarioDirectory().resolve( ScenarioHelper.USUAL_EVALUATION_FILE_NAME );
        String args[] = { config.toString() };
        Set<Path> paths = Collections.emptySet();

        try ( Control wresEvaluation = new Control( SYSTEM_SETTINGS,
                                                    DATABASE,
                                                    EXECUTOR ) )
        {
            ExecutionResult result = wresEvaluation.apply( args );

            if ( result.failed() )
            {
                throw new RuntimeException( "Execution resulted in exception.",
                                            result.getException() );
            }

            paths = wresEvaluation.get();
        }

        return paths;
    }
    

    /**
     * Checks for output validity from WRES and fails if not.  This is used in conjunction
     * with {@link #assertOutputsMatchBenchmarks(ScenarioInformation, Control)}.
     * @param@ completedEvaluation The {@link Control} that executed the evaluation.
     */
    private static void assertWRESOutputValid( ScenarioInformation scenarioInfo,
                                               Set<Path> initialOutputSet )
    {
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
                    fail( "Not all outputs of WRES Control.applyAsInt were written to the same directory.  "
                          + "That is not allowed in system testing." );
                }
            }
            
            LOGGER.info( "For scenario " + scenarioInfo.getName()
                         + " all outputs were written to "
                         + firstPath.getParent() );
        }

        //Anything else to validate about the output?
    }

    protected static void assertOutputsMatchBenchmarks( ScenarioInformation scenarioInfo,
                                                        Set<Path> initialOutputSet )
    {
        LOGGER.info( "Asserting that outputs match benchmarks for {}...", scenarioInfo.getName() );
        
        //Assert the output as being valid and then get the output from the provided Control if so.
        assertWRESOutputValid( scenarioInfo, initialOutputSet );

        //Create the directory listing... Temporarily removed for due to sorting issues.
        //Path dirListingPath;
        try
        {    
            //Redmine #51654-387 decided not to compare the dirListing.txt due to sorting issues.
            //dirListingPath = constructDirListingFile( initialOutputSet );
            
            // Need to filter out the *.png and the *.nc files and add to the hash set.
            HashSet<Path> finalOutputSet = new HashSet<Path>();
            for ( Path nextPath : initialOutputSet )
            {
                if ( nextPath.endsWith( ".png" ) || nextPath.endsWith( ".nc" ) )
                {
                    LOGGER.debug( "This file is binary and will not be compared with a benchmark: {}", nextPath );
                }
                else
                {
                    LOGGER.debug( "Benchmark comparison will include this file: {}", nextPath );
                    finalOutputSet.add( nextPath );
                }
            }
            
            //Do not check the dirListing for now, see Redmine #51654-387
            //finalOutputSet.add( dirListingPath );

            //Make the comparison and check the result code to ensure its 0.  
            //TODO Implement a better means of handling this.  Do we need a result code with JUnit system testing?
            int resultCode = compareOutputAgainstBenchmarks( scenarioInfo,
                                                         finalOutputSet );
            assertEquals( "Comparison with benchmarks failed with code " + resultCode + ".", 0, resultCode );
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
     * Constructs the dirListing.txt file for the outputs generated.<br>
     * <br>
     * Currently, this is not used, but is left in place in case it proves useful down the road.
     * @param generatedOutputs It is assumed that all outputs are generated in the same directory.
     * @return The {@link Path} to the directory listing file created.
     * @throws IOException
     */
    @SuppressWarnings( "unused" )
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
    private static int compareOutputAgainstBenchmarks( ScenarioInformation scenarioInfo,
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
            File benchmarkFile = identifyBenchmarkFile( outputFilePath, benchmarksPath );
            if ( benchmarkFile != null )
            {
                LOGGER.debug("Benchmark file comparison performed equivalent to:  diff " + outputFilePath + " " + benchmarkFile.getAbsolutePath() );
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
                        //LOGGER.warn("The pairs file differ from benchmark (result code " + pairResultCode + ") for file with name " + outputFileName);
                        LOGGER.warn("The pairs file differ from " + benchmarkFile.getAbsolutePath() + " (result code " + pairResultCode + ") for file with name " + outputFileName);
                    }
                    //Otherwise just do the comparison without sorting.
                    else if ( outputFileName.endsWith( ".csv" ) )
                    {
                        metricCSVResultCode = 32;
                        //LOGGER.warn("The metric CSV file differs from benchmark (result code " + metricCSVResultCode + ") for file with name " + outputFileName);
                        LOGGER.warn("The metric CSV file differs from " + benchmarkFile.getAbsolutePath() + " (result code " + metricCSVResultCode + ") for file with name " + outputFileName);
                    }
                    else if ( outputFileName.endsWith( ".txt" ) )
                    {
                        txtResultCode = 4;
                        //LOGGER.warn("The text file differs from benchmark (result code " + txtResultCode + ") for file with name " + outputFileName);
                        LOGGER.warn("The text file differs from " + benchmarkFile.getAbsolutePath() + " (result code " + txtResultCode + ") for file with name " + outputFileName);
                    }
                    else
                    {
                        miscResultCode = 2;
                        LOGGER.warn("A miscellaneous result returned (result code" + miscResultCode + ") for file with name " + outputFileName);
                    }
                }
                //Remove the benchmark as one to check.
                benchmarkedFiles.remove( benchmarkFile.getName() );
            }
            else 
            {
                LOGGER.debug("For output file, " + outputFilePath + ", benchmark file does not exist or could not otherwise be identified.  Skipping.");
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
            LOGGER.trace("Compare output file " + outputFile.getName() + " line " + i + " with benchmarks file " + benchmarkFile.getName());
            LOGGER.trace("Are they equal? " + actualRows.get( i ).equals(expectedRows.get( i )));
            //int expectedRowsIndex = expectedRows.indexOf(actualRows.get( i ));
            //LOGGER.info("Are they equals ? " + actualRows.get( i ).equals(expectedRows.get( expectedRowsIndex )));
            assertEquals( "For output file, " + outputFile.getName()
                          + ", row "
                          + i
                          + " differs from benchmark.",
                          actualRows.get( i ).trim(), expectedRows.get( i ).trim() );
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
            LOGGER.trace("Compare output file " + pairsFile.getName() + " line " + i + " with benchmarks file " + benchmarkFile.getName());
            LOGGER.trace("Are they equal? " + actualRows.get( i ).equals(expectedRows.get( i )));
            assertEquals( "For pairs file file, " + pairsFile.getName()
                          + ", after sorting alphabetically, row "
                          + i 
                          + " differs from benchmark.",
                          actualRows.get( i ).trim(), //TODO Should this and next be trimmed?
                          expectedRows.get( i ).trim() );
        }
    }

    
    /**
     * Search for a string within a file and replace it.
     * @param fileName The file to search.
     * @param searchFor The string searched for.
     * @param replace Its replacement.
     */
    static void searchAndReplace( String fileName, String searchFor, String replace)
    {
        File file = Paths.get( System.getProperty( "wres.dataDirectory" ) + "/" + fileName ).toFile();
        
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
                        LOGGER.info( "Replaced line " + lineNumber + " to " + aLine + " in file " + file.toString() );
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
     * @return The directory where system test scenario directories are found, which is
     * currently the working directory.
     */
    protected static Path getBaseDirectory()
    {
        return new File(System.getProperty( "user.dir" )).toPath();
    }

}

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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Sets;

import wres.control.Control;
import wres.io.Operations;

/**
 * A class to be used to run system testing scenarios of the WRES.  The class makes use of environment variables
 * to identify the system tests directory (which will be the working directory for executions), WRES database
 * information, and the logging level.  It then sets up appropriate Java system properties before running the
 * WRES.  After construction, the methods to call are then controlled from the external caller.  The choices
 * are provided as public methods below.
 * @author Raymond.Chui
 * @author Hank.Herr
 *
 */
public class SystestsScenarioRunner
{
    /**
     * The path to the system tests directory.
     */
    private Path systemTestsDirPath = null;

    /**
     * The name of the scenario.
     */
    private String scenarioName = null;

    /**
     * A {@link File} to specify the scenario directory.
     */
    private File scenarioDir = null;

    /**
     * The {@link Control} instance that is used to run WRES.
     */
    private Control wresControl = null;


    /**
     * @param systemTestsDir  A {@link Path} specifying the system testing directory.
     * @param scenarioName The name of a scenario corresponding to a subdirectory within the system testing directory.
     */
    public SystestsScenarioRunner( String scenarioName )
    {
        this.systemTestsDirPath = Paths.get( System.getenv( "TESTS_DIR" ) );
        this.scenarioName = scenarioName;
        this.scenarioDir = new File( this.systemTestsDirPath.toFile(), this.scenarioName );
        setAllPropertiesFromEnvVars();
    }

    /**
     * Sets the properties which drive the system testing.  These use system environment variables in order
     * to set Java system properties.
     */
    private void setAllPropertiesFromEnvVars()
    {
        //I was thinking about moving these to the SystemTestSuiteRunner, since they will always result
        //in the same properties across all of the suite tests.  However, I want to allow for individual
        //execution of the system tests, which would not be done via the suite.  For that reason, I'm
        //leaving it here.

	/*
        //TODO Modify this later if we ever change how the outputs are directed to a different
        //tmp directory.
	*/

	String dbHostFromEnvVar = System.getenv( "WRES_DB_HOSTNAME" );
	String dbHostFromSysProp = System.getProperty( "wres.url" );

	if ( dbHostFromSysProp == null && dbHostFromEnvVar != null
	     && !dbHostFromEnvVar.isEmpty() )
	{
	    System.setProperty( "wres.url", dbHostFromEnvVar );
	}

	String dbNameFromEnvVar = System.getenv( "WRES_DB_NAME" );
	String dbNameFromSysProp = System.getProperty( "wres.databaseName" );

	if ( dbNameFromSysProp == null && dbNameFromEnvVar != null
	     && !dbNameFromEnvVar.isEmpty() )
        {
	    System.setProperty( "wres.databaseName", dbNameFromEnvVar );
	}

	String dbUserFromEnvVar = System.getenv( "WRES_DB_USERNAME" );
	String dbUserFromSysProp = System.getProperty( "wres.username" );

	if ( dbUserFromSysProp == null && dbUserFromEnvVar != null
	     && !dbUserFromEnvVar.isEmpty() )
	{
	    System.setProperty( "wres.username", dbUserFromEnvVar );
	}

	// Passphrase should be got from postgres passphrase file. -Jesse
	// I thinks it's too late to attempt to set log level here. -Jesse

        System.getProperty( "java.io.tmpdir", System.getenv( "TESTS_DIR" ) + "/" + scenarioName );

        System.out.println( "Properties used to run test:" );
        System.out.println( "    wres.hostname = " + System.getProperty( "wres.hostname" ) );
        System.out.println( "    wres.url = " + System.getProperty( "wres.url" ) );
        System.out.println( "    wres.databaseName = " + System.getProperty( "wres.databaseName" ) );
        System.out.println( "    wres.username = " + System.getProperty( "wres.username" ) );
        System.out.println( "    wres.logLevel =  " + System.getProperty( "wres.logLevel" ) );
        System.out.println( "    wres.password =  " + System.getProperty( "wres.password" ) );
        System.out.println( "    user.dir (working directory) =  " + System.getProperty( "user.dir" ) );
        System.out.println( );
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
                if ( files[i].startsWith( "wres_evaluation_output" ) )
                {
                    Path outputPath = directoryToLookIn.resolve( files[i] );
                    System.out.println( "Deleting old system testing output directory, "
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
     * Execute the named project configuration file and assert a successful execution with valid
     * output.
     *
     * @param projectConfigFileName
     * @return The exit code returned by the call to method {@link Control#apply(String[])}.
     * @throws IOException If the project configuration file cannot be read.  This likely indicates that the
     * system test was not setup properly; hence the exception instead of a result code.
     */
    public void assertProjectExecution()
    {
        //Dump the contents of the project configuration file to a String.
        String xmlString = "";
        try
        {
            byte[] encoded;
            Path configFilePath = Paths.get( scenarioDir.getAbsolutePath(), "project_config.xml" );
            encoded = Files.readAllBytes( configFilePath );
            xmlString = new String( encoded );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "Unable to read project configuration into String: " + e.getMessage() );
        }

        //Execute the control and return the exit code if its not zero.  No need to go further.
        wresControl = new Control();

        //Note that this command may error out essentially ending the JUnit execution.
        //However, if it doesn't error out and, instead, the exit code indicates a problem,
        //that will be caught in the assert that follows.
        int exitCode = (int) wresControl.apply( new String[] { xmlString } );
        assertEquals( "Execution of WRES failed with exit code " + exitCode
                      + "; see log for more information!",
                      0,
                      exitCode );

        //Assert valid output.
        assertWRESOutputValid();
    }

    /**
     * Checks for output validity from WRES and fails if not.
     */
    private void assertWRESOutputValid()
    {
        //Obtain the complete list of outputs generated.
        Set<Path> initialOutputSet = wresControl.get();

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
    public void assertOutputsMatchBenchmarks()
    {
        Set<Path> initialOutputSet = wresControl.get();
        Path dirListingPath;
        try
        {
            dirListingPath = constructDirListingFile( initialOutputSet );
            HashSet<Path> finalOutputSet = Sets.newHashSet( initialOutputSet );
            finalOutputSet.add( dirListingPath );

            int resultCode;
            resultCode = compareOutputAgainstBenchmarks( finalOutputSet );
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
    private Path constructDirListingFile( Set<Path> generatedOutputs ) throws IOException
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
    private int compareOutputAgainstBenchmarks( Set<Path> generatedOutputs ) throws IOException, IllegalStateException
    {
        //Establish the benchmarks directory and obtain a listing of all benchmarked files.
        Path benchmarksPath = Paths.get( scenarioDir.getAbsolutePath(), "benchmarks" );
        File benchmarksDir = benchmarksPath.toFile();
        List<String> benchmarkedFiles = new ArrayList<>();
        if ( benchmarksDir.exists() && benchmarksDir.isDirectory() )
        {
            benchmarkedFiles.addAll( Arrays.asList( benchmarksDir.list() ) );
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
                    System.out.println( e.getMessage() );
                    if ( outputFileName.endsWith( "pairs.csv" ) )
                    {
                        pairResultCode = 16;
                    }
                    //Otherwise just do the comparison without sorting.
                    else if ( outputFileName.endsWith( ".csv" ) )
                    {
                        metricCSVResultCode = 32;
                    }
                    else if ( outputFileName.endsWith( ".txt" ) )
                    {
                        txtResultCode = 4;
                    }
                    else
                    {
                        miscResultCode = 2;
                    }
                }
                //Remove the benchmark as one to check.
                benchmarkedFiles.remove( benchmarkFile.getName() );
            }
        }
        if ( !benchmarkedFiles.isEmpty() )
        {
            System.out.println( "The following benchmarked files were not present in the evaluation output directory: "
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
    private File identifyBenchmarkFile( Path outputFilePath, Path benchmarkDirPath )
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
    private void assertOutputTextFileMatchesExpected( File outputFile, File benchmarkFile ) throws IOException
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
            assertEquals( "For output file, " + outputFile.getName()
                          + ", row "
                          + i
                          + " differs from benchmark.",
                          actualRows.get( i ).trim(),
                          expectedRows.get( i ).trim() );
        }
    }

    /**
     * Asserts that the output pairs are equal to a benchmark file, if one exists.
     * @param pairsFile
     * @param benchmarkDirPath
     * @throws IOException
     */
    private void assertOutputPairsEqualExpectedPairs( File pairsFile, File benchmarkFile ) throws IOException
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
    public boolean doAfter( String[] files )
    {
        boolean isABeforeScript = false;
        for ( int i = 0; i < files.length; i++ )
        {
            if ( files[i].startsWith( "after.sh" ) )
            {
                isABeforeScript = true;
                System.out.println( "Found " + files[i] );
                searchAndReplace( System.getProperty( "user.dir" ) + "/" + files[i] );
            }
        }
        return isABeforeScript;
    }

    /**
    * if there is a before script, do it 1st
    * @param files -- a list of files
    * @return -- false if there is no before script; Otherwise, true
    */
    public boolean doBefore( String[] files )
    {
        boolean isABeforeScript = false;
        for ( int i = 0; i < files.length; i++ )
        {
            if ( files[i].startsWith( "before.sh" ) )
            {
                isABeforeScript = true;
                System.out.println( "Found " + files[i] );
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
    public void searchAndReplace( String fileName, String searchFor, String replace, String line )
    {
        File file = Paths.get( System.getProperty( "user.dir" ) + "/" + fileName ).toFile();
        /*
        System.out.println(file.toString() + '\n' +
                searchFor + '\n' +
		replace + '\n' +
		line);
        */
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
                        System.out.println( "Replaced line " + lineNumber + " to " + aLine );
                    }
                    arrayList.add( aLine );
                    lineNumber++;
                }
                bufferedReader.close();
                lineNumber = 0;
                BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter( file ) );
                for ( Iterator<String> iterator = arrayList.iterator(); iterator.hasNext(); lineNumber++ )
                {
                    //System.out.println(lineNumber + ": " + iterator.next().toString());
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
    public void searchAndReplace( String fileName )
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
                        //System.out.println("File = " + theFile[1]);
                    }
                    else if ( ( index = aLine.lastIndexOf( "Search=" ) ) > 0 )
                    {
                        search = aLine.substring( index ).split( "=" );
                        //System.out.println("Search = " + search[1]);
                    }
                    else if ( ( index = aLine.lastIndexOf( "Replace=" ) ) > 0 )
                    {
                        replace = aLine.substring( index ).split( "=" );
                        //System.out.println("Replace = " + replace[1]);
                    }
                    else if ( ( index = aLine.lastIndexOf( "Line=" ) ) > 0 )
                    {
                        line = aLine.substring( index ).split( "=" );
                        //System.out.println("Line = " + line[1]);
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
} // end this class

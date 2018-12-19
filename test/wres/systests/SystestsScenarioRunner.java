package wres.systests;


import static junit.framework.TestCase.assertEquals;
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
    * Wrapper on {@link Operations#cleanDatabase()}.
     * @throws SQLException 
     * @throws IOException 
    */
    public static void assertCleanDatabase()
    {
        try
        {
            Operations.cleanDatabase();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "IOException occurred while cleaning the database: " + e.getMessage() );
        }
        catch ( SQLException e )
        {
            e.printStackTrace();
            fail( "IOException occurred while cleaning the database: " + e.getMessage() );
        }
    }

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
        //TODO Move these to the SystemTestSuiteRunner... if we choose that approach.
        
        Properties props = System.getProperties();
        props.setProperty( "wres.hostname", System.getenv( "WRES_DB_HOSTNAME" ) );
        props.setProperty( "wres.url", System.getenv( "WRES_DB_HOSTNAME" ) );
        props.setProperty( "wres.databaseName", System.getenv( "WRES_DB_NAME" ) );
        props.setProperty( "wres.username", System.getenv( "WRES_DB_USERNAME" ) );
        props.setProperty( "wres.logLevel", System.getenv( "WRES_LOG_LEVEL" ) );
        props.setProperty( "wres.password", System.getenv( "WRES_DB_PASSWORD" ) );

        System.out.println( "wres.hostname = " + System.getProperty( "wres.hostname" ) );
        System.out.println( "wres.url = " + System.getProperty( "wres.url" ) );
        System.out.println( "wres.databaseName = " + System.getProperty( "wres.databaseName" ) );
        System.out.println( "wres.username = " + System.getProperty( "wres.username" ) );
        System.out.println( "wres.logLevel =  " + System.getProperty( "wres.logLevel" ) );
        System.out.println( "wres.password =  " + System.getProperty( "wres.password" ) );
        System.out.println( "user.dir (working directory) =  " + System.getProperty( "user.dir" ) );
    }


    /**
    * Delete wres_evaluation_output_* from previous run.
    * @param testScenarioDir The directory in which to look for evaluation output subdirectories.
    * @return True if anything is deleted, false otherwise.
    * @throws IOException 
    */
    public void assertDeletionOfOldOutputDirectories()
    {
        String[] files = scenarioDir.list();

        //Search the files for anything that appears to be wres evaluation output and remove the entire directory.
        try
        {
            for ( int i = 0; i < files.length; i++ )
            {
                if ( files[i].startsWith( "wres_evaluation_output" ) )
                {
                    Path outputPath =
                            FileSystems.getDefault().getPath( scenarioDir.getCanonicalPath() + "/" + files[i] );
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

        //TODO When the below fails, JUnit stops, but I'm not sure why.  For example, if I break the config
        //by pointed to wrong data, it just stops the failure trace reporting an InternalWresException
        // "Could not complete project execution".  Why?  I purposely check the exit code coming out of apply
        //to catch problems below.  

        int exitCode = (int) wresControl.apply( new String[] { xmlString } );

        //Assert the run is good.
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
        //Obtain the complete list of outputs generated and confirm that all paths are in the same directory.
        Set<Path> initialOutputSet = wresControl.get();
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
     * This confirms that all of the outputs generated by the run of {@link Control#apply(String[])} were written
     * to the same directory.  It  builds the dirListing.txt file for that directory and then compares all of the 
     * outputs.  Anything in the output directory that has a corresponding benchmark will be diffed.  If anything
     * in the benchmarks is not found in the outputs, then a difference is reported; this should be equivalent to 
     * check dirListing.txt, in that something in the benchmarks but not in the output should result in a dirListing.txt
     * difference, but I wanted to be certain nothing fell through the cracks.  <br>
     * <br>
     * If exceptions are thrown when calling this method, it indicates something basic when wrong that is not 
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
        int overallResultCode = 0;

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
        for ( Path outputFilePath : generatedOutputs )
        {
            String outputFileName = outputFilePath.toFile().getName();
            int resultCode = 0;

            //For the pairs, you need to sort them first.
            if ( outputFileName.endsWith( "pairs.csv" ) )
            {
                String sortedFileName = "sorted_" + outputFileName;
                File sortedFile = new File( outputFilePath.getParent().toFile(), sortedFileName );
                File benchmarkFile = new File( benchmarksPath.toFile(), sortedFileName );
                sortPairs( outputFilePath.toFile(), sortedFile );
                resultCode = compareFiles( sortedFile, benchmarkFile );
                benchmarkedFiles.remove( sortedFileName );
            }
            //Otherwise just do the comparison.
            else
            {
                File outputFile = outputFilePath.toFile();
                File benchmarkFile = new File( benchmarksPath.toFile(), outputFileName );
                resultCode = compareFiles( outputFile, benchmarkFile );
            }

            overallResultCode = Math.max( overallResultCode, resultCode );
            benchmarkedFiles.remove( outputFileName );
        }
        if ( !benchmarkedFiles.isEmpty() )
        {
            System.out.println( "The following benchmarked files were not present in the evaluation output directory: "
                                + Arrays.toString( benchmarkedFiles.toArray() ) );
            return 2;
        }

        return overallResultCode;
    }

    /**
     * Comparing two files, line by line.
     * @param evaluationFile The evaluation output file.
     * @param benchmarksFile The benchmarks file
     * @return 0 no errors; 2 output not found; 4 sort failed; 8 found diff in txt files; 16 found diff in sorted_pairs; 32 found diff in csv files
     */
    private int compareFiles( File evaluationFile, File benchmarksFile )
    {
        //TODO Need to look into this routine.  Can it just assert multiple times and report all differences found?
        //Right now it uses a result code (which I think may be incorrectly implemented, by the way... I think its 
        //supposed to add the different code not just take one number) which captures multiple differences in a single
        //value.

        int returnValue = 0;

        //If the evaluationFile does not exist, something very weird happened.  
        if ( !evaluationFile.exists() || !evaluationFile.isFile() )
        {
            throw new IllegalStateException( "The evaluation output file " + evaluationFile.getAbsolutePath()
                                             + " does not exist or is not a file.  Why did you ask me to compare it?" );
        }

        //If the benchmark file doesn't exist, then its not being tracked.  Don't worry about it.
        //If it does, diff it.
        if ( benchmarksFile.exists() && benchmarksFile.isFile() )
        {
            try
            {
                BufferedReader evaluationReader = new BufferedReader( new FileReader( evaluationFile ) );
                BufferedReader benchmarksReader = new BufferedReader( new FileReader( benchmarksFile ) );

                String evaluationLine = "";
                String benchmarksLine = "";
                returnValue = 0;
                while ( ( evaluationLine = evaluationReader.readLine() ) != null )
                {
                    if ( ( benchmarksLine = benchmarksReader.readLine() ) != null )
                    {
                        //if (evaluationLine.compareTo( benchmarksLine ) != 0) {
                        if ( evaluationLine.equals( benchmarksLine ) == false )
                        {
                            if ( evaluationFile.getName().endsWith( ".txt" ) )
                                returnValue = 4; //dirListing.txt
                            else if ( evaluationFile.getName().endsWith( "sorted_pairs.csv" ) )
                                returnValue = 16; //pairs difference
                            else if ( evaluationFile.getName().endsWith( ".csv" ) )
                                returnValue = 32; //metric output difference
                            else
                                returnValue = 2; //other file type
                        }
                    }
                }
                benchmarksReader.close();
                evaluationReader.close();
            }
            catch ( FileNotFoundException fnfe )
            {
                returnValue = 2;
                System.err.println( fnfe.getMessage() );
            }
            catch ( IOException ioe )
            {
                returnValue = 2;
                System.err.println( ioe.getMessage() );
            }
        }
        return returnValue;
    }

    /**
     * Sort the pairs.csv file and write output to sorted_pairs.csv 
     * @param pairsFile -- pairs.csv file path
     * @throws IOException If a problem is encountered during sorting.
     */
    private void sortPairs( File pairsFile, File sortFile ) throws IOException
    {
        //TODO Look at what James proposed in #51654.  Should be able to load and compare
        //pairs in memory after sorting.  Would avoid the need to write the file.  On the
        //other hand, I wonder if that sorted_pairs.csv file is actually useful to keep?

        System.out.println( "pairs file = " + pairsFile.getAbsolutePath() );

        //Read in the pairs.
        ArrayList<PairsFilePair> pairsList = new ArrayList<PairsFilePair>();
        String firstLine;
        try ( BufferedReader pairsReader = new BufferedReader( new FileReader( pairsFile ) ) )
        {
            String aLine = "";
            firstLine = aLine = pairsReader.readLine(); // skip the first line
            while ( ( aLine = pairsReader.readLine() ) != null )
            {
                pairsList.add( new PairsFilePair( aLine ) );
            }
            pairsReader.close(); //TODO Does the try automatically handle closing?  If so, remove this.
        }

        //Sort the pairs list.
        Collections.sort( pairsList, new SortedPairsComparator() );

        // After sort, write the file back out.
        try ( BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter( sortFile.getAbsolutePath() ) ) )
        {
            bufferedWriter.write( firstLine );
            bufferedWriter.newLine();
            for ( Iterator<PairsFilePair> iterator = pairsList.iterator(); iterator.hasNext(); )
            {
                PairsFilePair pair = iterator.next();
                pair.writeToPairsFile( bufferedWriter );
            }
            bufferedWriter.flush();
            bufferedWriter.close(); //TODO Does the try automatically handle closing?  If so, remove this.
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

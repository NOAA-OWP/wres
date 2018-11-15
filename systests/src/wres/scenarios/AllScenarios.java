package wres.scenarios;


import org.junit.Test;

import wres.Main;
//import wres.MainFunctions;
import wres.io.Operations;
//import wres.config.generated.*;
import wres.control.Control;

import java.sql.SQLException;

import java.util.*;
//import java.lang.*;

//import wres.control.Control;
//import wres.system.SystemSettings;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.file.*;

public class AllScenarios extends Main {
    
    public AllScenarios() {
        super();
        
       
        System.setProperty( "wres.hostname", System.getenv( "WRES_DB_HOSTNAME" ) );
        System.setProperty( "wres.databaseName", System.getenv( "WRES_DB_NAME" ) );
        System.setProperty( "wres.username", System.getenv( "WRES_DB_USERNAME" ));
        System.setProperty( "wres.logLevel", System.getenv("WRES_LOG_LEVEL") );
        
        System.out.println( "wres.hostname = " + System.getProperty( "wres.hostname" ) );
        System.out.println( "wres.databaseName = " + System.getProperty( "wres.databaseName" ));
        System.out.println( "wres.username = " + System.getProperty( "wres.username" ) );
        System.out.println( "wres.logLevel =  " + System.getProperty( "wres.logLevel" ) );
    }
/*
    @Test public void testTheMain() {
    //public void testTheMain() {
        //Main testMain = new Main();
        assertNotNull("get version",
        getVersion());
      //  new HelloTest();
       // testScenarios();
    }
*/    
    /**
     * Run a scenario test
     * @param dir -- scenario test directory name
     */
    public Path runTest(String dir) {
        System.setProperty( "user.dir", System.getenv( "TESTS_DIR" ) + "/" + dir );
        Path userdirPath = Paths.get(System.getProperty("user.dir"));
        File testDir = userdirPath.toFile();
        System.out.println("testDir = " + testDir.getAbsolutePath());
        
        System.out.println( "PWD = " + System.getProperty( "user.dir" ) );
        //System.setProperty( dir, testDir.getAbsolutePath() );
      
        String files[] = testDir.list();
        
        //System.out.println( "wres.hostname = " + System.getProperty( "wres.hostname" ) );
        
        // Do clean database if there is a CLEAN file
        for (int i = 0; i < files.length; i++) {
            //System.out.println( files[i] );
            if (files[i].endsWith( "CLEAN" )) {
                System.out.println( "cleaning the database " + System.getProperty( "wres.databaseName" )  );
               
                try {
                    Operations.cleanDatabase();
                } catch (IOException e1) {
                    System.err.println( "IOException: " + e1.getMessage() );
                } catch (SQLException e2) {
                    System.err.println( "SQLException: " + e2.getMessage() );
                } 
            }
        }  
        
        // delete wres_evaluation_output_* from previous run.
        for (int i = 0; i < files.length; i++) {
            //if (files[i].startsWith( "wres_evaluation_output_" )) {
             if (files[i].startsWith( "wres_evaluation_output" )) {
                System.out.println( "Need to delete files under output directory ");
                Path outputPath = FileSystems.getDefault().getPath( testDir.getAbsolutePath() + "/" + files[i]);
         
                File outputDir = outputPath.toFile();
                String outputFiles[] = outputDir.list();
                    for (int j = 0; j < outputFiles.length; j++) {
                        Path outputFile = FileSystems.getDefault().getPath( outputDir.getAbsolutePath() + "/" + outputFiles[j]);
                        try {
                            Files.delete( outputFile );
                            //System.out.println( "Deleted file " + outputFile.toString() );
                        } catch (IOException ioe) {
                            System.err.println( ioe.toString() );
                        }
                    }
                
                try {
                    Files.deleteIfExists( outputPath );
                    System.out.println( "Deleted directory " +  outputPath.toString());
                } catch (IOException ioe) {
                    System.err.println( ioe.toString() );
                }
            }
        }
        Path tmppath = null;
        // search for project_config.xml file and execute it
        for (int i = 0; i < files.length; i++) {
            if (files[i].endsWith( "project_config.xml" )) {
                System.out.print( "executing this config file: " + files[i]);
                
                
                String executeFile [] = {files[i]};
           
                try {
                    Control control = new Control();
                    
                    //System.setProperty( "java.io.tmpdir", System.getProperty( "user.dir"));
                    //System.out.println( "java.io.tmpdir before apply = " + System.getProperty( "java.io.tmpdir" ) );
                    // Above java.io.tmpdir setProperty may or may not work in Eclipse! 
                    
                    Integer integer = control.apply( executeFile );
                    
                    //System.out.println( "java.io.tmpdir after apply = " + 
                    //                        System.getProperty( "java.io.tmpdir" ) );
                    System.out.println( "execute returns " + integer.intValue() );
                    
                 // if the java.io.tmpdir couldn't create under test director, then 
                 // will let it in /tmp, then move it to test directory. It's silly! 
                    
                    Set<Path> hashSet = control.get();
                    if (hashSet.isEmpty() == false) {
                        Iterator<Path> iterator = hashSet.iterator();
                        if (iterator.hasNext()) {
                            Path path = iterator.next();
                            System.out.println("the tmp path = " 
                                        + path.getParent().toString());
                            File tmpdir = path.getParent().toFile();
                            
                            /*Path tmppath = Paths.get( System.getProperty( "user.dir" ) + 
                                                      "/" + 
                                    path.getParent().getFileName().toString() ); */
                            tmppath = Paths.get( System.getProperty( "user.dir" ) + 
                                                      "/" + 
                                    path.getParent().getFileName().toString() );
                            //System.out.println( "destination tmp path = " + tmppath.toString());
                            if (tmpdir.isDirectory()) {
                                try {
                                    Files.createDirectory( tmppath);
                                    System.out.println( "created tmpdir at " +
                                        tmppath.toString());
                                    
                                    //List of files in tmpdir
                                    File [] tmpfiles = tmpdir.listFiles();
                                    for (int j = 0; j < tmpfiles.length; j++) {
                                        Path tmpfile = Paths.get( tmppath.toString() + "/" 
                                                            + tmpfiles[j].getName() ); 
                                        Files.move( tmpfiles[j].toPath(), tmpfile, 
                                                    StandardCopyOption.REPLACE_EXISTING);
                                        if (System.getenv( "WRES_LOG_LEVEL" ).compareTo( "debug" ) == 0)
                                            System.out.println( "Moved " + tmpfiles[j].toPath().toString() + 
                                                            " to " + tmpfile.toString());
                                    } // end this for loop
                                    tmpdir.delete();
                                    System.out.println( "Deleted directory " + tmpdir.toString() );
                                    
                                } catch (FileAlreadyExistsException faee) {
                                    //System.err.println( faee.getMessage() );
                                    System.err.println( tmppath.toString() + " already exists");
                                } 
                            } // end if
                            // do file comparison vs. (versus) benchmarks
                            //fileComparison(tmppath);
                        } // end if iterator has.next()
                    } // end if hashSet.isEmpty
                    //hashSet.clear();
                    //hashSet = null;
                } catch (NullPointerException npe) {
                    System.err.println( "java.io.tmpdir: NullPointerException: " + npe.getMessage() );
                } catch (Exception e) {
                    e.printStackTrace();
                }                
            } // end if for search project_config.xml
        } // end for loop
        return tmppath;
    } // end runTest method
    
    /**
     * Compare the evaluation *.csv files with benchmarks
     * @param evaluationPath -- evaluation directory path
     */
    public void fileComparison(Path evaluationPath) {
        File evaluationDir = evaluationPath.toFile();
        String [] evaluationFileNames = evaluationDir.list();
        
        Hashtable<String, Path> benchmarksTable = new Hashtable<String, Path>();
        //benchmarksTable.clear();
        String [] benchmarksFileNames = null;
        
        Path benchmarksPath = Paths.get( System.getProperty( "user.dir" ) + "/" + "benchmarks" );
        File benchmarksDir = benchmarksPath.toFile();
        
        if (benchmarksDir.exists() && benchmarksDir.isDirectory()) {
            System.out.println( "Benchmarks Directory = " + benchmarksDir.toString() );
            benchmarksFileNames = benchmarksDir.list();
        }
                
        for (int i = 0; i < benchmarksFileNames.length; i++) {
            if (benchmarksFileNames[i].endsWith( ".csv" )) {
                benchmarksTable.put( benchmarksFileNames[i], 
                                     Paths.get(benchmarksPath.toString() + 
                                               "/" + benchmarksFileNames[i]));
            }
        }
        if (benchmarksTable.isEmpty() == false) {
            for (int i = 0; i < evaluationFileNames.length; i++) {
                if (evaluationFileNames[i].endsWith( ".csv" )) {
                    if (evaluationFileNames[i].startsWith( "pairs" )) {
                        sortPairs(Paths.get(evaluationPath.toString() + "/" 
                                            + evaluationFileNames[i]).toFile());
                        // After sorted, its new name will be sorted_pairs.csv
                        evaluationFileNames[i] = "sorted_pairs.csv";
                    }
                    if (benchmarksTable.get( evaluationFileNames[i]) != null) {
                        File benchmarksFile = benchmarksTable.get( evaluationFileNames[i]).toFile();
                        
                        File evaluationFile = Paths.get( evaluationPath.toString() + 
                                                  "/" + evaluationFileNames[i]).toFile();
                        if (compareTheFiles(evaluationFile, benchmarksFile) == false) {
                            System.err.println( "there are differences between " + 
                                    evaluationFile.toString()
                                    //+ "/" + evaluationFile.getName()
                                    + " and " +
                                    benchmarksFile.toString());
                        } else {
                            if(System.getenv("WRES_LOG_LEVEL").compareTo("debug") == 0)
                                System.out.println( "file  " + 
                                    evaluationFile.toString() + " is the same as " +
                                    benchmarksFile.toString());
                        }
                    } else {
                        System.err.println( "Benchmarks file " + evaluationFileNames[i] 
                                + " not found." );
                    } // end else
                } // end outer if
            } // end for loop
        } // end if benchmarks is empty
        else {
            System.err.println("No benchmarks file matched");
        }
    } // end file comparison method
    
    /**
     * Comparing two files 
     * @param evaluationFile -- file
     * @param benchmarksFile -- and benchmarks file
     * @return true if no differences. Otherwise return false
     */
    public boolean compareTheFiles(File evaluationFile, File benchmarksFile) {
        
        boolean returnValue;
                        
        if(evaluationFile.exists() && evaluationFile.isFile()) {
            if (benchmarksFile.exists() && benchmarksFile.isFile()) {
                /*
                System.out.println( "Ready to compare evaluation file : " 
                                    + evaluationFile.toString() + 
                                    " with benchmarks file " +
                                    benchmarksFile.toString());
                                    */
                try {
                    BufferedReader evaluationReader = new BufferedReader(new FileReader(evaluationFile));
                    BufferedReader benchmarksReader = new BufferedReader(new FileReader(benchmarksFile));
                    
                    String evaluationLine = "";
                    String benchmarksLine = "";
                    returnValue = true;
                    while ((evaluationLine = evaluationReader.readLine()) != null) {
                        if ((benchmarksLine = benchmarksReader.readLine()) != null) {
                            //if (evaluationLine.compareTo( benchmarksLine ) != 0) {
                            if (evaluationLine.equals( benchmarksLine ) == false) {
                                returnValue = false;                               
                                /* System.out.println( "DEBUG lines: " + returnValue + "\n" 
                                                    + evaluationLine + "\n" + 
                                                    benchmarksLine); */
                            }
                        }                        
                    }
                    benchmarksReader.close();
                    evaluationReader.close();
                } catch (FileNotFoundException fnfe) {
                    returnValue = false;
                    System.err.println( fnfe.getMessage() );
                } catch (IOException ioe) {
                    returnValue = false;
                    System.err.println( ioe.getMessage() );
                }
            } else {
                System.err.println( "Benchmarks file not exists " + benchmarksFile.toPath().toString());
                returnValue = false;
            }
        } else {
            System.err.println( "Evaluation file not exists " + evaluationFile.toPath().toString());
            returnValue = false;
        }
        return returnValue;
    } // end of this method
    
    /**
     * Sort the pairs.csv file and write output to sorted_pairs.csv 
     * @param pairsFile -- pairs.csv file path
     */
    public void sortPairs(File pairsFile) {
        
        // System.out.println( pairsFile.getAbsolutePath() );
        try {
            BufferedReader pairsReader = new BufferedReader(new FileReader(pairsFile));
            String aLine = "";
            ArrayList<ThePairs> arrayList = new ArrayList<ThePairs>();
            String firstLine = aLine = pairsReader.readLine(); // skip the first line
            while ((aLine = pairsReader.readLine()) != null) {
                //StringTokenizer stringTokenizer = new StringTokenizer(aLine, ",");
                String[] delimiter = aLine.split( "," );
                ArrayList<String> list = new ArrayList<String>();
                for (int i = 4; i < delimiter.length; i++) {
                    list.add(delimiter[i]);
                }
                arrayList.add( new ThePairs(
                                     delimiter[0],
                                     delimiter[1],                                   
                                      Integer.parseInt( delimiter[2] ),
                                      Integer.parseInt( delimiter[3]),
                                      Double.parseDouble( delimiter[4]),
                                      list
                                      ));
            }
            pairsReader.close();
            Collections.sort( arrayList, new  SortedPairs());
            
            // After sorted, let's write to the sorted_pairs.csv file
            String sortedPairs = pairsFile.getParent() + "/" + "sorted_pairs.csv";
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(sortedPairs));
           
            for (Iterator<ThePairs> iterator = arrayList.iterator(); iterator.hasNext();) {
                ThePairs thePairs = iterator.next();
                
                bufferedWriter.write( 
                        thePairs.getSiteID() + "," +
                        thePairs.getValidTime() +  "," +                              
                        thePairs.getLeadTimeInSeconds().intValue() + "," +
                        thePairs.getTimeWindowIndex().intValue()
                        );
                for (Iterator<String> iterator2 = thePairs.getArrayList().iterator();
                        iterator2.hasNext();) {
                    bufferedWriter.write("," + 
                    iterator2.next().toString());
                }
                bufferedWriter.newLine();
            }
            bufferedWriter.write( firstLine);
            bufferedWriter.flush();
            bufferedWriter.close();
            System.out.println( "sorted to " + sortedPairs );
        } catch (FileNotFoundException fnfe) {
            System.err.println( fnfe.getMessage() );
        } catch (IOException ioe) {
            System.err.print( ioe.getMessage() );
        }
    } // end this sortPairs method    
} // end this class

class ThePairs {
    private String siteID;
    private String validTime;
    private Double left;
    private Integer leadTimeInSeconds;
    private Integer timeWindowIndex;
    private ArrayList<String> arrayList;
    
    public ThePairs(
                    String siteID,
                    String validTime,
                    Integer leadTimeInSeconds, 
                    Integer timeWindowIndex,
                    Double left,
                    ArrayList<String> arrayList
            ) {
        
        this.siteID = siteID;
        this.validTime = validTime;
        this.left = left;
        this.leadTimeInSeconds = leadTimeInSeconds;
        this.timeWindowIndex = timeWindowIndex;
        this.arrayList = arrayList;
        
    } // end this construction
    
    public String getSiteID() { return this.siteID; }
    public String getValidTime() { return this.validTime; }
    public Double getLeft() { return this.left; }
    public Integer getLeadTimeInSeconds() { return this.leadTimeInSeconds; }
    public Integer getTimeWindowIndex() { return this.timeWindowIndex; }
    public ArrayList<String> getArrayList() { return this.arrayList; }
    
} // end ThePairs class

class SortedPairs implements Comparator<ThePairs> {
    
    // Override the Comparator.compare
    @Override 
    public int compare(ThePairs tp1, ThePairs tp2) {
            int leadTimeInSeconds = tp1.getLeadTimeInSeconds().compareTo( tp2.getLeadTimeInSeconds());
                if (leadTimeInSeconds == 0) {
                    int timeWindowIndex = tp1.getTimeWindowIndex().intValue() - tp2.getTimeWindowIndex().intValue();
                    if (timeWindowIndex == 0) {
                        int validTime = tp1.getValidTime().compareTo( tp2.getValidTime());
                        if (validTime == 0) {
                            double left = tp1.getLeft().doubleValue() - tp2.getLeft().doubleValue();
                            if (left == 0.0) {
                                return validTime;
                            } else return validTime;
                        } else return timeWindowIndex;
                    } else return leadTimeInSeconds;
            } else return leadTimeInSeconds;
        //} else return validTime;
    } // end this method
} // end the SortedPairs class

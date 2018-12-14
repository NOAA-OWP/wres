//package wres.scenarios;


//import wres.Main;
//import wres.MainFunctions;
import wres.io.Operations;
//import wres.config.generated.*;
import wres.control.Control;

import java.sql.SQLException;

import java.util.*;

import java.io.*;
import java.nio.file.*;

/**
 * Run the scenario tests by JUnit with gradle
 * Usage: ./gradlew run --args='scenario###'
 * Where ### is the scenario series number
 *
 * @author <a href="mailto:Raymond.Chui@***REMOVED***">
 * @version 1.5
 * Created: November, 2018
 */
 
public class AllScenarios {
    
    public AllScenarios() {
	//System.out.println("Ready to run the scenarios.");
	setAllProperties();
    }
    public void setAllProperties() {    
       
        System.setProperty( "wres.hostname", System.getenv( "WRES_DB_HOSTNAME" ) );
        System.setProperty( "wres.databaseName", System.getenv( "WRES_DB_NAME" ) );
        System.setProperty( "wres.username", System.getenv( "WRES_DB_USERNAME" ));
        System.setProperty( "wres.logLevel", System.getenv("WRES_LOG_LEVEL") );
        
        System.out.println( "wres.hostname = " + System.getProperty( "wres.hostname" ) );
        System.out.println( "wres.databaseName = " + System.getProperty( "wres.databaseName" ));
        System.out.println( "wres.username = " + System.getProperty( "wres.username" ) );
        System.out.println( "wres.logLevel =  " + System.getProperty( "wres.logLevel" ) );
    }
    
	public static void main(String [] args) {
		try {
		//Path path = new AllScenarios().runTest(args[0]);
			for (int i = 0; i < args.length; i++) {
				new AllScenarios().runTest(args[i]);
			}
			System.out.println("Done all");
			System.exit(0);
		} catch (ArrayIndexOutOfBoundsException aie) {
			System.err.println(aie.getMessage());
		}
	}

    /**
    * Clean the database if there is a CLEAN file
    * @param files -- a list of files
    * @return -- false if no cleaning; true otherwise
    */
    public boolean cleanDatabase(String[] files) {
	boolean isClean = false;
	for (int i = 0; i < files.length; i++) {
		if (files[i].endsWith( "CLEAN" )) {
                	System.out.println( "cleaning the database " + System.getProperty( "wres.databaseName" )  );
			isClean = true;
                	try {
                		Operations.cleanDatabase();
                	} catch (IOException e1) {
                    		System.err.println( "IOException: " + e1.getMessage() );
                	} catch (SQLException e2) {
                    		System.err.println( "SQLException: " + e2.getMessage() );
                	}
            	}
	}
	return isClean;
    }

    /**
    * delete wres_evaluation_output_* from previous run.
    * @param files -- a list of files
    * @param testDir -- a test directory
    * @return -- false if no deleted; true otherwise
    */
    public boolean deleteOldEvaluationDirectory(String[] files, File testDir) {
	boolean isDeleted = false;
	// delete wres_evaluation_output_* from previous run.
	for (int i = 0; i < files.length; i++) {
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
                    } // end inner for loop
		try {
                    Files.deleteIfExists( outputPath );
                    System.out.println( "Deleted directory " +  outputPath.toString());
                } catch (IOException ioe) {
                    System.err.println( ioe.toString() );
                }
		isDeleted = true;
              } // end if
	} // end outer for loop
	return isDeleted;
    } // end method

    /**
    * if there is a before script, do it 1st
    * @param files -- a list of files
    * @return -- false if there is no before script; Otherwise, true
    */
    public boolean doBefore(String[] files) {
	boolean isABeforeScript = false;
	for (int i = 0; i < files.length; i++) {
		if (files[i].startsWith( "before.sh" )) {
			isABeforeScript = true;
			System.out.println("Found " + files[i]);
			searchAndReplace(System.getProperty("user.dir") + "/" + files[i]);
		}
	}
	return isABeforeScript;
    }

    /**
    * if there is a after script, do it now 
    * @param files -- a list of files
    * @return -- false if there is no after script; Otherwise, true
    */
    public boolean doAfter(String[] files) {
        boolean isABeforeScript = false;
        for (int i = 0; i < files.length; i++) {
                if (files[i].startsWith( "after.sh" )) {
                        isABeforeScript = true;
			System.out.println("Found " + files[i]);
			searchAndReplace(System.getProperty("user.dir") + "/" + files[i]);
                }
        }
        return isABeforeScript;
    }

    /**
     * Run a scenario test
     * @param dir -- scenario test directory name
     * @return -- the tmpdir path
     */
    public void runTest(String dir) {
	//setAllProperties();
        System.setProperty( "user.dir", System.getenv( "TESTS_DIR" ) + "/" + dir );
        Path userdirPath = Paths.get(System.getProperty("user.dir"));
        File testDir = userdirPath.toFile();
        System.out.println("testDir = " + testDir.getAbsolutePath());
        
        System.out.println( "PWD = " + System.getProperty( "user.dir" ) );
        //System.setProperty( dir, testDir.getAbsolutePath() );
      
        String files[] = testDir.list();
	cleanDatabase(files);
        deleteOldEvaluationDirectory(files, testDir);
	doBefore(files);
	executeConfigFile(files);
	doAfter(files);
     }
     
     public void executeConfigFile(String[] files) { 
        // Path tmppath = null;
        // search for project_config.xml file and execute it
        for (int i = 0; i < files.length; i++) {

            if (files[i].endsWith( "project_config.xml" )) {
        	StringBuffer stringBuffer = concatenateFile(files[i]);        
                //String executeFile [] = {System.getProperty("user.dir") + "/" + files[i]};
                //System.out.println( "executing this config file: " + System.getProperty("user.dir") + "/" + files[i]);
                String executeFile [] = {stringBuffer.toString()};
           
                try {
                    Control control = new Control();
                    
                    //System.setProperty( "java.io.tmpdir", System.getProperty( "user.dir"));
                    // System.setProperty( "java.io.tmpdir", "/tmp");
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
                            
                            Path tmppath = Paths.get( System.getProperty( "user.dir" ) + 
                                                      "/" + 
                                    path.getParent().getFileName().toString() );
                            /* tmppath = Paths.get( System.getProperty( "user.dir" ) + 
                                                      "/" + 
                                    path.getParent().getFileName().toString() ); */
                            System.out.println( "destination tmp path = " + tmppath.toString());
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
                            //System.out.println( "check the tmp path = " + tmppath.toString());
                            int ResultCode = fileComparison(tmppath);
							System.out.println("Files comparison result code: " + ResultCode);
                        } // end if iterator has.next()
						// control = null;
                    } // end if hashSet.isEmpty
                } catch (NullPointerException npe) {
                    System.err.println( "java.io.tmpdir: NullPointerException: " + npe.getMessage() );
                } catch (Exception e) {
                    e.printStackTrace();
                }                
            } // end if for search project_config.xml
        } // end for loop
		System.out.println("Done with runtest");
    } // end runTest method
    
/**
 * concatenate a file and append the lines into a string buffer and return it.
 *
 * @param fileName -- concatenate this file name
 * @return the contents of a file in a string buffer
 */
    public StringBuffer concatenateFile(String fileName) {
	String line = "";
	StringBuffer stringBuffer = new StringBuffer();
	String fullPath = System.getProperty("user.dir") + "/" + fileName;
	System.out.println("Execute: " + fullPath);
	try {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(fullPath)));
		while ((line = bufferedReader.readLine()) != null) {
			stringBuffer.append(line);
		}
		bufferedReader.close();
	 } catch (FileNotFoundException fnfe) {
		System.err.println( fnfe.getMessage() );
	} catch (IOException ioe) {
		System.err.println( ioe.getMessage() );
	}
	return stringBuffer;
    }


    /**
     * Compare the evaluation *.csv files with benchmarks
     * @param evaluationPath -- evaluation directory path
     * @return 0 no errors; 2 output not found; 4 sort failed; 8 found diff in txt files; 16 found diff in sorted_pairs; 32 found diff in csv files
     */
    public int fileComparison(Path evaluationPath) {
		
		int ResultCode = 0;
        File evaluationDir = evaluationPath.toFile();
        String [] evaluationFileNames = evaluationDir.list();
        
        Hashtable<String, Path> benchmarksTable = new Hashtable<String, Path>();
        //benchmarksTable.clear();
        String [] benchmarksFileNames = null;
        
        Path benchmarksPath = Paths.get( System.getProperty( "user.dir" ) + "/" + "benchmarks" );
        File benchmarksDir = benchmarksPath.toFile();
        
        if (benchmarksDir.exists() && benchmarksDir.isDirectory()) {
            //System.out.println( "Benchmarks Directory = " + benchmarksDir.toString() );
            benchmarksFileNames = benchmarksDir.list();
        } else {
			System.err.println(benchmarksPath.toString() + " not found or isn't a directory");
			ResultCode = 2;
			return ResultCode;
		}
        //System.out.println("Number of benchmarks files = " + benchmarksFileNames.length);        
		// Save the *.csv and *.txt files from benchmarks directory into a table
        for (int i = 0; i < benchmarksFileNames.length; i++) {
            if (benchmarksFileNames[i].endsWith( ".csv" ) || 
					benchmarksFileNames[i].endsWith( ".txt" )) {
                benchmarksTable.put( benchmarksFileNames[i], 
                                     Paths.get(benchmarksPath.toString() + 
                                               "/" + benchmarksFileNames[i]));
            }
        }
        if (benchmarksTable.isEmpty() == false) {
            for (int i = 0; i < evaluationFileNames.length; i++) {
                if (evaluationFileNames[i].endsWith( ".csv" ) ||
						evaluationFileNames[i].endsWith( ".txt" )) {
                    if (evaluationFileNames[i].startsWith( "pairs" )) {
						// System.out.println("Ready to sort " + evaluationPath.toString() + "/" + evaluationFileNames[i]);
                        sortPairs(Paths.get(evaluationPath.toString() + "/" 
                                            + evaluationFileNames[i]).toFile());
                        // After sorted, its new name will be sorted_pairs.csv
                        evaluationFileNames[i] = "sorted_pairs.csv";
                    }
                    if (benchmarksTable.get( evaluationFileNames[i]) != null) {
                        File benchmarksFile = benchmarksTable.get( evaluationFileNames[i]).toFile();
                        
                        File evaluationFile = Paths.get( evaluationPath.toString() + 
                                                  "/" + evaluationFileNames[i]).toFile();
                        if ((ResultCode = compareTheFiles(evaluationFile, benchmarksFile)) != 0) {
                            System.err.println( "there are differences between " + 
                                    evaluationFile.toString()
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
						ResultCode = 2;
                    } // end else
                } // end outer if
            } // end for loop
        } // end if benchmarks is empty
        else {
            System.err.println("No benchmarks file matched");
			ResultCode = 2;
        }
		System.out.println("Done with fileComparison");
		return ResultCode;
    } // end file comparison method
    
    /**
     * Comparing two files 
     * @param evaluationFile -- file
     * @param benchmarksFile -- and benchmarks file
     * @return 0 no errors; 2 output not found; 4 sort failed; 8 found diff in txt files; 16 found diff in sorted_pairs; 32 found diff in csv files
     */
    public int compareTheFiles(File evaluationFile, File benchmarksFile) {
        
        int returnValue = 0;
                        
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
                    returnValue = 0;
                    while ((evaluationLine = evaluationReader.readLine()) != null) {
                        if ((benchmarksLine = benchmarksReader.readLine()) != null) {
                            //if (evaluationLine.compareTo( benchmarksLine ) != 0) {
                            if (evaluationLine.equals( benchmarksLine ) == false) {
								if (evaluationFile.getName().endsWith(".txt"))
									returnValue = 4;
								else if (evaluationFile.getName().endsWith("sorted_pairs.csv"))
									returnValue = 16;
								else if (evaluationFile.getName().endsWith(".csv"))
									returnValue = 32;
								else
                                	returnValue = 2;                               
                                /* System.out.println( "DEBUG lines: " + returnValue + "\n" 
                                                    + evaluationLine + "\n" + 
                                                    benchmarksLine); */
                            }
                        }                        
                    }
                    benchmarksReader.close();
                    evaluationReader.close();
                } catch (FileNotFoundException fnfe) {
                    returnValue = 2;
                    System.err.println( fnfe.getMessage() );
                } catch (IOException ioe) {
                    returnValue = 2;
                    System.err.println( ioe.getMessage() );
                }
            } else {
                System.err.println( "Benchmarks file not exists " + benchmarksFile.toPath().toString());
                returnValue = 2;
            }
        } else {
            System.err.println( "Evaluation file not exists " + evaluationFile.toPath().toString());
            returnValue = 2;
        }
        return returnValue;
    } // end of this method

	/**
	* Search and replace a string in a file
	* @param fileName -- file to read and write
	* @param searchFor -- a string to search for
	* @param replace -- a string to replace
	* @param line -- a specify line number search/replace, or 'g' for global
	*/
	public void searchAndReplace(String fileName, String searchFor, String replace, String line) {
		File file = Paths.get(System.getProperty("user.dir") + "/" + fileName).toFile();
		/*
		System.out.println(file.toString() + '\n' +
			searchFor + '\n' +
			replace + '\n' +
			line);
		*/
		if (file.exists()) {
			try {
				ArrayList<String> arrayList = new ArrayList<String>();
				BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
				String aLine;
				int lineNumber = 0;
				while ((aLine = bufferedReader.readLine()) != null) {
					int index = 0;
					if ((index = aLine.indexOf(searchFor)) >= 0) {
						aLine = aLine.replaceAll(searchFor, replace);
						System.out.println("Replaced line " + lineNumber + " to " + aLine);
					}
					arrayList.add(aLine);
					lineNumber++;
				}
				bufferedReader.close();
				lineNumber = 0;
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
				for (Iterator<String> iterator = arrayList.iterator(); 
						iterator.hasNext(); lineNumber++) {
					//System.out.println(lineNumber + ": " + iterator.next().toString());
					bufferedWriter.write(iterator.next().toString());
					bufferedWriter.newLine();
				}
				bufferedWriter.flush();
				bufferedWriter.close();
			} catch (FileNotFoundException fnfe) {
                                System.err.println( fnfe.getMessage());
                        } catch (IOException ioe) {
                                System.err.print( ioe.getMessage());
                        }
		} else {
                        System.err.println("File " + file.getPath() + " doesn't existed.");
                }
 
	}

	/**
 	* Search for token File=, Search=, Replace=, and Line= from a file
 	* @param fileName -- a before.sh or after.sh shell script
 	*/
	public void searchAndReplace(String fileName) {
		File file = Paths.get(fileName).toFile();
		if (file.exists()) {
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
				String aLine;
				String[] theFile = null;
				String[] search = null;
				String[] replace = null;
				String[] line = null;
				while ((aLine = bufferedReader.readLine()) != null) {
					int index = 0;
					if ((index = aLine.lastIndexOf("File=")) > 0) {
						theFile = aLine.substring(index).split("=");
						//System.out.println("File = " + theFile[1]);
					} else if ((index = aLine.lastIndexOf("Search=")) > 0) {
						search = aLine.substring(index).split("=");
						//System.out.println("Search = " + search[1]);
					} else if ((index = aLine.lastIndexOf("Replace=")) > 0) {
						replace = aLine.substring(index).split("=");
						//System.out.println("Replace = " + replace[1]);
					} else if ((index = aLine.lastIndexOf("Line=")) > 0) {
						line = aLine.substring(index).split("=");
						//System.out.println("Line = " + line[1]);
					}
				} // end while loop
				bufferedReader.close();
				searchAndReplace(theFile[1].trim(), search[1].trim(), replace[1].trim(), line[1].trim());
			} catch (FileNotFoundException fnfe) {
				System.err.println( fnfe.getMessage());
			} catch (IOException ioe) {
				System.err.print( ioe.getMessage());
			} 
		} else {
			System.err.println("File " + file.getPath() + " doesn't existed.");
		} 
	} // end method

    
    /**
     * Sort the pairs.csv file and write output to sorted_pairs.csv 
     * @param pairsFile -- pairs.csv file path
     */
    public void sortPairs(File pairsFile) {
        
        System.out.println("pairs file = " + pairsFile.getAbsolutePath() );
        try {
            BufferedReader pairsReader = new BufferedReader(new FileReader(pairsFile));
            String aLine = "";
            ArrayList<ThePairs> arrayList = new ArrayList<ThePairs>();
            String firstLine = aLine = pairsReader.readLine(); // skip the first line
            while ((aLine = pairsReader.readLine()) != null) {
                //StringTokenizer stringTokenizer = new StringTokenizer(aLine, ",");
                String[] delimiter = aLine.split( "," );
                ArrayList<String> list = new ArrayList<String>();
				
                for (int i = 9; i < delimiter.length; i++) {
                    list.add(delimiter[i]);
                }
				
                arrayList.add( new ThePairs(
                                     delimiter[0], // siteID
                                     delimiter[1], // earlestIssueTime
                                     delimiter[2], // latestIssueTime
                                     delimiter[3], // earlestValidTime
                                     delimiter[4], // latestValieTime
                                      Integer.parseInt( delimiter[5] ), // earlestLeadTimeInSeconds
                                      Integer.parseInt( delimiter[6]), // latestLeadTimeInSeconds
									  delimiter[7], // validTimePairs
                                      Integer.parseInt( delimiter[8]), // leadDurationPairInSeconds
                                      list // leftAndrightMemberInCMS
                                      ));
            }
            pairsReader.close();
			//System.out.println("ready to sort " + arrayList.size());
            Collections.sort( arrayList, new  SortedPairs());
			//System.out.println("sorted");
            
            // After sorted, let's write to the sorted_pairs.csv file
            String sortedPairs = pairsFile.getParent() + "/" + "sorted_pairs.csv";
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(sortedPairs));

            bufferedWriter.write( firstLine);
			bufferedWriter.newLine();
            for (Iterator<ThePairs> iterator = arrayList.iterator(); iterator.hasNext();) {
                ThePairs thePairs = iterator.next();
                

                bufferedWriter.write( 
                        thePairs.getSiteID() + "," + // 1
                        thePairs.getEarlestIssueTime() +  "," + // 2 
                        thePairs.getLatestIssueTime() +  "," +  // 3 
                        thePairs.getEarlestValidTime() +  "," + // 4 
                        thePairs.getLatestValieTime() +  "," +  // 5 
                        thePairs.getEarlestLeadTimeInSeconds().intValue() + "," +
                        thePairs.getLatestLeadTimeInSeconds().intValue() + "," +
                        thePairs.getValidTimePairs() +  "," +
                        thePairs.getLeadDurationPairInSeconds().intValue()
                        );
                for (Iterator<String> iterator2 = thePairs.getLeftAndRightMemberInCMS().iterator(); iterator2.hasNext();) {
                    bufferedWriter.write("," + 
                     iterator2.next().toString());
                }
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            bufferedWriter.close();
            //System.out.println( "sorted to " + sortedPairs );
        } catch (FileNotFoundException fnfe) {
            System.err.println( fnfe.getMessage());
		} catch (NumberFormatException nfe) {
			System.err.println(nfe.getMessage());
        } catch (IOException ioe) {
            System.err.println( ioe.getMessage());
        }
	System.out.println("end of sortPairs");
    } // end this sortPairs method    
} // end this class

package wres.systests;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import nl.wldelft.util.timeseries.DefaultTimeSeriesHeader;
import nl.wldelft.util.timeseries.ParameterType;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import ohd.hseb.hefs.utils.tools.FileTools;
import ohd.hseb.hefs.utils.tsarrays.TimeSeriesArraysTools;
import ohd.hseb.hefs.utils.tsarrays.agg.AggregationTools;
import ohd.hseb.hefs.utils.tsarrays.agg.TimeSeriesAggregationException;
import ohd.hseb.util.misc.HString;

public abstract class SystestBenchmarkChecker
{

    public static void main( String[] args )
    {

        try
        {
            //Get a listing of the directory and sort alphabetically the found file names.
            //The sorting should result in files sorted by issue time.
            File baseDir = new File( "../systests/data/drrc2ForecastsOneMonth/" );
            Set<File> filesList =
                    FileTools.listFilesWithSuffix( baseDir,
                                                   ".xml" );
            List<String> fileNames = new ArrayList<>();
            for (File file : filesList)
            {
                fileNames.add( file.getName() );
            }
            Collections.sort( fileNames );

            //Store list of all output.
            List<String> listOfAllOutput = new ArrayList<>();
            
            //Loop through each file name...
            for ( String fileName : fileNames )
            {
                //Read the ensemble.
                TimeSeriesArrays tss = TimeSeriesArraysTools.readFromFile( FileTools.newFile( baseDir, fileName ) );
                List<TimeSeriesArray> aggTSS = new ArrayList<>();
                
                //Aggregate each time series to desired aggregation and add to a list.
                for ( int i = 0; i < tss.size(); i++ )
                {
                    TimeSeriesArray ts = tss.get( i );
                    ( (DefaultTimeSeriesHeader) ts.getHeader() ).setParameterType( ParameterType.ACCUMULATIVE ); //Ensure a 4-point mean.
                    TimeSeriesArray newArray =
                            AggregationTools.aggregateToMean( ts,
                                                              ts.getForecastTime(),
                                                              ts.getEndTime(),
                                                              "4 hour",
                                                              null,
                                                              null,
                                                              false,
                                                              false );
                    aggTSS.add( newArray );
                }

                //NOTE THE LEAD TIME RESTRICTION!  I'm essentially defining the lead time window within the for loop.
                //With the files sorted alphabetically, that should imply sorting by issue time, which should (?) lead to 
                //identical ordering with the sorted_pairs.csv.  
                DecimalFormat df = new DecimalFormat("0.00000");
                for ( int leadTimeIndex = 6; leadTimeIndex < 12; leadTimeIndex++ )
                {
                    //Get the value for the lead time across all members.
                    List<Double> valuesForLeadTime = new ArrayList<>();
                    for ( int i = 0; i < aggTSS.size(); i++ )
                    {
                        TimeSeriesArray ts = aggTSS.get( i );
                        valuesForLeadTime.add( (double) ts.getValue( leadTimeIndex ) );
                    }
                    
                    //Sort by value.
                    Collections.sort( valuesForLeadTime );
                    
                    //Convert to formatted string and then put the strings together using a tool I wrote years ago since
                    //I'm not familiar with the newer Java tools.  The number format defined above is used.
                    List<String> strings = valuesForLeadTime.stream().map(op -> df.format( op )).collect(Collectors.toList());
                    listOfAllOutput.add( HString.buildStringFromList( strings, "," ) );
                }
            }
            
            //Dump the lines.
            for (String line : listOfAllOutput)
            {
                System.err.println( line );
            }
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( InterruptedException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( TimeSeriesAggregationException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }


}

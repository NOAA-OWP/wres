package wres.systests;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import evs.io.nws.misc.HString;
import nl.wldelft.util.timeseries.DefaultTimeSeriesHeader;
import nl.wldelft.util.timeseries.ParameterType;
import nl.wldelft.util.timeseries.TimeSeriesArray;
import nl.wldelft.util.timeseries.TimeSeriesArrays;
import ohd.hseb.hefs.utils.tools.FileTools;
import ohd.hseb.hefs.utils.tsarrays.TimeSeriesArraysTools;
import ohd.hseb.hefs.utils.tsarrays.agg.AggregationTools;
import ohd.hseb.hefs.utils.tsarrays.agg.TimeSeriesAggregationException;
import ohd.hseb.util.Pair;
import ohd.hseb.util.misc.HCalendar;

/**
 * This test is used to generate sorted_pairs.csv output based on PI-timeseries XML.  This is for the "right" values in the pairs.
 * I have not written anything for the "left" values.
 * 
 * @author Hank.Herr
 *
 */
public abstract class SystestBenchmarkChecker
{

    public static void main( String[] args ) throws IOException,
            InterruptedException, TimeSeriesAggregationException
    {
        //=================================================================
        // List XML files.
        //=================================================================
        
        //Get a listing of the directory and sort alphabetically the found file names.
        //The sorting should result in files sorted by issue time.  The sorting is important
        //since it will dictate the ordering of the output.
        File baseDir = new File( "../systests/data/tmp801/" );
        List<File> allFiles = new ArrayList<File>();
        allFiles.addAll(
                         FileTools.listFilesWithSuffix( baseDir,
                                                        ".xml" ) );
        allFiles.addAll(
                         FileTools.listFilesWithSuffix( baseDir,
                                                        ".xml.gz" ) );
        Collections.sort( allFiles, new Comparator<File>()
        {
            @Override
            public int compare( File arg0, File arg1 )
            {
                return arg0.getName().compareTo( arg1.getName() );
            }
        } );

        //Convert list to strings and sort again, just to be sure.
        List<String> fileNames = new ArrayList<>();
        for ( File file : allFiles )
        {
            fileNames.add( file.getName() );
        }
        Collections.sort( fileNames );

        //=================================================================
        //Read the observations... a single time series.
        //=================================================================
        
        //Read and store the observations.
        TimeSeriesArrays obsTSS = TimeSeriesArraysTools.readFromFile( FileTools.newFile( baseDir, "../CKLN6_STG.xml" ) );
        TimeSeriesArray obsTS = obsTSS.get( 0 );
        

        //=================================================================
        //Read and aggregated forecast time series and compute corresponding observations.
        //=================================================================
        //Store list of all output.
        List<String> listOfAllOutput = new ArrayList<>();

        //Date format for output column in results.
        SimpleDateFormat dateFormatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        dateFormatter.setTimeZone( TimeZone.getTimeZone( "GMT" ) );

        //Map of results, which are stored as strings to be output to pairs file.
        Map<Pair<Date, Integer>, List<String>> resultsMap = new LinkedHashMap<>();

        //Loop through each file name...
        for ( String fileName : fileNames )
        {
            //Read the ensemble.
            TimeSeriesArrays tss = TimeSeriesArraysTools.readFromFile( FileTools.newFile( baseDir, fileName ) );
            List<TimeSeriesArray> aggTSS = new ArrayList<>();

            //Aggregate each time series to desired aggregation and add to a list.  The aggregation below
            //assumes all members read in from file have the same time parameters.
            for ( int i = 0; i < tss.size(); i++ )
            {
                TimeSeriesArray ts = tss.get( i );
                ( (DefaultTimeSeriesHeader) ts.getHeader() ).setParameterType( ParameterType.ACCUMULATIVE ); //Ensure a 4-point mean.
                TimeSeriesArray newArray =
                        AggregationTools.aggregateToMaximum( ts,                                             //This is set to 801 desired time scale: Max from Day 3 - 6.
                                                          ts.getForecastTime() + 48 * 1000 * 60 * 60,  
                                                          ts.getForecastTime() + (48 + 96) * 1000 * 60 * 60, //OLD - ts.getEndTime(),
                                                          "96 hour",
                                                          null,
                                                          null,
                                                          false,
                                                          false );
                aggTSS.add( newArray );

            }
            
            //Aggregate the observation time series over the same time span as the above. 
            //XXX: I COULD NOT GET THE OBSERVATIONS TO ALWAYS COME OUT RIGHT WHEN CHECKING 801.  For some reason, some are NaN
            //and I don't have time to debug, so for 801 I just used Excel.
            TimeSeriesArray aggregatedObservedArray =
                    AggregationTools.aggregateToMaximum( obsTS,                                               
                                                      tss.get(0).getForecastTime() + 48 * 1000 * 60 * 60,  
                                                      tss.get(0).getForecastTime() + (48 + 96) * 1000 * 60 * 60, //OLD - ts.getEndTime(),
                                                      "96 hour",
                                                      null,
                                                      null,
                                                      false,
                                                      false );
            
            //NOTE THE LEAD TIME RESTRICTION!  I'm essentially defining the lead time window within the for loop.
            //With the files sorted alphabetically, that should imply sorting by issue time, which should (?) lead to
            //identical ordering with the sorted_pairs.csv.
            DecimalFormat df = new DecimalFormat( "0.00000" );

            //The for loop controls which aggregation lead times are included in the results map.  
            for ( int leadTimeIndex = 0; leadTimeIndex <= 0; leadTimeIndex += 1 ) //For 801, only one lead time is paired.
            {
                //Get the value for the lead time across all members.
                List<Double> valuesForLeadTime = new ArrayList<>();
                for ( int i = 0; i < aggTSS.size(); i++ )
                {
                    TimeSeriesArray ts = aggTSS.get( i );
                    valuesForLeadTime.add( (double) ts.getValue( leadTimeIndex ) );
                }
                
                //Observe value is pulled from the aggregated observations.
                double obsValue = aggregatedObservedArray.getValue( leadTimeIndex );

                //Sort by value.
                Collections.sort( valuesForLeadTime );

                //Convert to formatted string and then put the strings together using a tool I wrote years ago since
                //I'm not familiar with the newer Java tools.  The number format defined above is used.
                //The observed value is inserted into the list before putting in the map.
                List<String> strings =
                        valuesForLeadTime.stream().map( op -> df.format( op ) ).collect( Collectors.toList() );
                strings.add( 0, df.format( obsValue ) );
                resultsMap.put( new Pair<Date, Integer>( new Date( aggTSS.get( 0 ).getTime( leadTimeIndex ) ),
                                                         1 ), //There is only one lead time, so the pairs file will always use 1!  Here is the old value: (leadTimeIndex + 1) * 3 ),
                                strings );
            }
        }

        //=================================================================
        //Pool the results!
        //=================================================================
        //Result map is ready with pairs for each time series that could be pooled.  
        //Output while loop will be for the issuedDatesPoolingWindow
        //Inner loop for the leadTimesPoolingWindow.

        //Prep the outer loop.  Initial settings dictated by issuedDatesPoolingWindow period and issuedDates start.
        //This is the first window:
        Calendar windowStartDate = HCalendar.processDate( "1900-06-01 12:00:00" ); //For 801, set for all ts included. OLD - HCalendar.processDate( "1985-06-01 12:00:00" );
        Calendar windowEndDate = HCalendar.processDate( "2100-06-01 12:00:00" ); //Set for all ts included. OLD - HCalendar.processDate( "1985-06-01 12:00:00" );
        windowEndDate.add( Calendar.DAY_OF_YEAR, 21 );

        //Overall end is dictated by the issuedDates end.
        Calendar overallEndDate = HCalendar.processDate( "2100-06-30 12:00:00" ); //Set for all ts included.  OLD - HCalendar.processDate( "1985-06-30 12:00:00" );

        //Overall window index to output to pairs results.
        int windowIndex = 1;

        //Loop it.  Note the start date must be before the overall issuedDates end date.
        while ( windowStartDate.before( overallEndDate ) )
        {
            //Prep the inner loop.  Dictated by the leadTimePoolingWidnows period, (start, end], and leadHours start.
            int leadStartTime = 0;
            int leadEndTime = 600; //OLD = 6;  For scenario801, just make it big!

            //Loop it.  End of loop is dictated by leadHours in project config.
            while ( leadStartTime < 24 ) //801 - I'm looping only once so loop control does not matter.  See how its incremented below.
            {
                boolean somethingAdded = false;

                System.err.println( "WINDOW (" + dateFormatter.format( windowStartDate.getTime() ) + " - " + dateFormatter.format( windowEndDate.getTime() ) + "] ... (" + leadStartTime + ", " + leadEndTime + "]" );

                //Innermost loop: Go through all pairs, identify which are in the window, and include an output line for those that are.
                //This will result in duplications, but that's allowed in the pairs when pairs are in multiple windows.
                for ( Pair<Date, Integer> pair : resultsMap.keySet() )
                {
                    Calendar pairDate = HCalendar.computeCalendarFromDate( pair.first() );
                    int pairLead = pair.second();

                    if ( pairDate.getTimeInMillis() > windowStartDate.getTimeInMillis() &&
                         pairDate.getTimeInMillis() <= windowEndDate.getTimeInMillis() &&
                         pairLead > leadStartTime &&
                         pairLead <= leadEndTime )
                    {
                        listOfAllOutput.add( dateFormatter.format( pairDate.getTime() ) + ","
                                             + pairLead
                                             + ","
                                             + windowIndex
                                             + ","
                                             + HString.buildStringFromList( resultsMap.get( pair ), "," ) );
                        somethingAdded = true;
                    }
                }

                //Incrememnt lead time window according to leadTimesPoolingWindow frequency.
                leadStartTime += 1000; // 801 - Stop after one window.  So making this huge. OLD -  12;  
                leadEndTime += 1000; // 801 - Stop after one window.  So making this huge. OLD - 12;
                if (somethingAdded)
                {
                    windowIndex++;
                }
                else
                {
                    System.out.println( ">> NOTHING FOR THIS WINDOW!" );
                }
            }

            //Increment issued date widnow according to issedDatesPoolingWidnow frequency.
            windowStartDate.add( Calendar.DAY_OF_YEAR, 1000000 ); //801 - Stop after one window.  So making this huge. OLD - Calendar.DAY_OF_YEAR, 14 );
            windowEndDate.add( Calendar.DAY_OF_YEAR, 1000000 ); //801 - Stop after one window.  So making this huge. OLD - Calendar.DAY_OF_YEAR, 14 );
        }

        //Dump the lines.
        for ( String line : listOfAllOutput )
        {
            System.err.println( line );
        }
    }
}

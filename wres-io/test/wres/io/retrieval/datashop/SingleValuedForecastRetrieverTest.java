package wres.io.retrieval.datashop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Ignore;
import org.junit.Test;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.io.config.LeftOrRightOrBaseline;

/**
 * Class that illustrates reading of single-valued forecasts.
 * 
 * To run, remove the @Ignore, but do NOT commit any ignored tests.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedForecastRetrieverTest
{

    /**
     * Illustrates data access with generic DAOs. This illustration involves forecast time-series.
     * 
     * This test should NOT be committed without @Ignore.
     */
    @Ignore
    @Test
    public void testGetSomeTimeSeriesByTimeSeriesId()
    {
        // This test uses a live database instance. It is purely illustrative.
        // Providing there is some data in your wres database instance and the 
        // default system settings are correct or the wresconfig.xml was found,
        // then it should work. Do NOT commit without @Ignore

        // Goal 1: retrieve a stream of single-valued forecasts for given identifiers
        // Goal 2: retrieve a stream of single-valued forecasts for given identifiers 
        //         and a time window filter
        // No assertions made, purely illustrative

        // Create a units mapper for the desired measurement units
        UnitMapper mapper = UnitMapper.of( "CMS" );

        // Create the DAO to access the single-valued forecast data
        Retriever<TimeSeries<Double>> dao =
                new SingleValuedForecastRetriever.Builder().setUnitMapper( mapper ).build();

        // Define three time-series identifiers
        // These identifiers are completely arbitrary 
        // Only works if there is a database instance with these time-series identifiers
        long[] timeSeriesToRetrieve = new long[] { 1, 2, 3 };

        // Acquire a stream of (three) time-series
        // In this example, the data store is a database. A time-series will not 
        // be read from the database into memory until the stream is actually 
        // operated upon. In other words, the stream is simply a hook to time-series data
        Stream<TimeSeries<Double>> timeSeries = dao.get( LongStream.of( timeSeriesToRetrieve ) );

        // Stream the time-series into a collection
        List<TimeSeries<Double>> listOfSeries = timeSeries.collect( Collectors.toList() );

        assertEquals( 3, listOfSeries.size() );

        // Acquire a conditional stream of time-series, filtered by time window
        TimeWindow filter = TimeWindow.of( Duration.ofMinutes( 0 ), Duration.ofMinutes( 1080 ) );

        // Create the DAO for the filtered data
        // When these filters get numerous, a builder will help
        Retriever<TimeSeries<Double>> daoFiltered =
                new SingleValuedForecastRetriever.Builder().setUnitMapper( mapper )
                                                          .setTimeWindow( filter )
                                                          .build();

        Stream<TimeSeries<Double>> timeSeriesFiltered = daoFiltered.get( LongStream.of( timeSeriesToRetrieve ) );

        // Stream into a collection
        List<TimeSeries<Double>> listOfFilteredSeries = timeSeriesFiltered.collect( Collectors.toList() );

        assertEquals( 3, listOfFilteredSeries.size() );

        TimeSeriesRetriever<Double> timeSeriesData =
                new SingleValuedForecastRetriever.Builder().setProjectId( 1 )
                                                          .setVariableFeatureId( 1 )
                                                          .setUnitMapper( mapper )
                                                          .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.RIGHT )
                                                          .build();

        assertTrue( Arrays.equals( new long[] { 1, 2 }, timeSeriesData.getAllIdentifiers().toArray() ) );

    }


}

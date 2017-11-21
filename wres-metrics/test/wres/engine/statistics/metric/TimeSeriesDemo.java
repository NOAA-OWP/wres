package wres.engine.statistics.metric;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.RegularTimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.RegularTimeSeriesOfSingleValuedPairs.RegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.time.TimeSeries;

/**
 * Illustration of the @link TimeSeries} API using {@link RegularTimeSeriesOfSingleValudPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesDemo
{

    @Test
    public void demonstrateTimeSeries() throws MetricParameterException
    {
        //Build an immutable regular time-series of single-valued pairs
        DataFactory dataFactory = DefaultDataFactory.getInstance();
        RegularTimeSeriesOfSingleValuedPairsBuilder builder =
                dataFactory.ofRegularTimeSeriesOfSingleValuedPairsBuilder();
        //Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( "1985-01-01T00:00:00Z" );
        List<PairOfDoubles> firstValues = new ArrayList<>();
        //Add some values
        firstValues.add( dataFactory.pairOf( 1, 2 ) );
        firstValues.add( dataFactory.pairOf( 3, 4 ) );
        firstValues.add( dataFactory.pairOf( 5, 6 ) );
        //Set the timestep for the time-series to 6h
        Duration timeStep = Duration.ofHours( 6 );
        //Create some default metadata for the time-series
        Metadata metaData = dataFactory.getMetadataFactory().getMetadata();
        //Build the atomic time-series
        RegularTimeSeriesOfSingleValuedPairs timeSeries =
                (RegularTimeSeriesOfSingleValuedPairs) builder.addData( firstId, firstValues )
                                                              .setTimeStep( timeStep )
                                                              .setMetadata( metaData )
                                                              .build();
        //Take a look at the time-series

        //Start by simply printing the untimed values using the PairedInput API, since the 
        //TimeSeries is a SingleValuedPairs
        for ( PairOfDoubles next : timeSeries )
        {
            System.out.println( next );
        }
//        1.0,2.0
//        3.0,4.0
//        5.0,6.0

        //Next, print the time-series of values by valid time in ISO-8601
        System.out.println( timeSeries );
//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-01T18:00:00Z,5.0,6.0
        //Print the basis times associated with the time-series in ISO-8601
        System.out.println( timeSeries.getBasisTimes() );
//[1985-01-01T00:00:00Z]
        //Print the difference between the basis times and valid times, aka "lead times" for a forecast dataset
        //in ISO-8601. P qualifies a timespan, T separates date and time, H represents hour.
        System.out.println( timeSeries.getLeadTimes() );
//[PT6H, PT12H, PT18H]      

        //Add another atomic time-series to the builder and rebuild
        Instant secondId = Instant.parse( "1985-01-02T00:00:00Z" );
        List<PairOfDoubles> secondValues = new ArrayList<>();
        //Add some values
        secondValues.add( dataFactory.pairOf( 7, 8 ) );
        secondValues.add( dataFactory.pairOf( 9, 10 ) );
        secondValues.add( dataFactory.pairOf( 11, 12 ) );
        //Build the atomic time-series
        timeSeries = builder.addData( secondId, secondValues ).build();

        //Print the values by valid time
        System.out.println( timeSeries );
//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-01T18:00:00Z,5.0,6.0
//1985-01-02T06:00:00Z,7.0,8.0
//1985-01-02T12:00:00Z,9.0,10.0
//1985-01-02T18:00:00Z,11.0,12.0        

        //Iterate the atomic time-series by issue date/time
        for ( TimeSeries<PairOfDoubles> next : timeSeries.basisTimeIterator() )
        {
            System.out.println( next + System.lineSeparator() );
        }
//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-01T18:00:00Z,5.0,6.0
//
//1985-01-02T06:00:00Z,7.0,8.0
//1985-01-02T12:00:00Z,9.0,10.0
//1985-01-02T18:00:00Z,11.0,12.0

        //Iterate the atomic time-series by lead time
        for ( TimeSeries<PairOfDoubles> next : timeSeries.leadTimeIterator() )
        {
            System.out.println( next + System.lineSeparator() );
        }

//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-02T06:00:00Z,7.0,8.0
//
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-02T12:00:00Z,9.0,10.0
//
//1985-01-01T18:00:00Z,5.0,6.0
//1985-01-02T18:00:00Z,11.0,12.0        

        //Slice the time-series to obtain the atomic time-series with an issue time of 1985-01-02T00:00:00Z
        TimeSeries<PairOfDoubles> filteredOne =
                timeSeries.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) );
        System.out.println( filteredOne );

//1985-01-02T06:00:00Z,7.0,8.0
//1985-01-02T12:00:00Z,9.0,10.0
//1985-01-02T18:00:00Z,11.0,12.0        

        //Slice the time-series to obtain the atomic time-series with a lead time of 12 hours only
        TimeSeries<PairOfDoubles> filteredTwo =
                timeSeries.filterByLeadTime( a -> a.equals( Duration.ofHours( 12 ) ) );
        System.out.println( filteredTwo );
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-02T12:00:00Z,9.0,10.0

        //Slice the time-series to obtain the atomic time-series with an issue time of 1985-01-02T00:00:00Z 
        //and a lead time of 12 hours (i.e. filter chaining)
        TimeSeries<PairOfDoubles> filteredThree =
                timeSeries.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) )
                          .filterByLeadTime( b -> b.equals( Duration.ofHours( 12 ) ) );
        System.out.println( filteredThree );
//1985-01-02T12:00:00Z,9.0,10.0

        //Compute a verification metric for the TimeSeries, recalling that the TimeSeries is a SingleValuedPairs 
        MetricFactory metFac = MetricFactory.getInstance( dataFactory );
        MeanError me = metFac.ofMeanError();

        //Compute the mean error by lead time
        for ( TimeSeries<PairOfDoubles> next : timeSeries.leadTimeIterator() )
        {
            System.out.println( me.apply( (SingleValuedPairs) next ) );
        }
//1.0
//1.0
//1.0        

        //Example of an exceptional case: build a filter that produces in an irregular time-series (i.e. varying 
        //time-step), for which there is currently no concrete implementation
        try
        {
            timeSeries.filterByLeadTime( a -> a.equals( Duration.ofHours( 12 ) )
                                              || a.equals( Duration.ofHours( 18 ) ) );
        }
        catch ( UnsupportedOperationException e )
        {
            System.out.println( "While attempting to filter a time-series: "+ e.getMessage());
        }

    }

}

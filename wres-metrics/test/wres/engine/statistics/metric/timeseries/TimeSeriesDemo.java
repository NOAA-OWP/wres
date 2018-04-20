package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.singlevalued.MeanError;

/**
 * Illustration of the {@link TimeSeries} API.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesDemo
{


    /**
     * Instance of a data factory.
     */

    private DataFactory dataFactory = null;


    /**
     * Instance of a slicer.
     */

    private Slicer slicer = null;


    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        dataFactory = DefaultDataFactory.getInstance();
        slicer = dataFactory.getSlicer();
    }

    @Test
    public void demonstrateTimeSeries() throws MetricParameterException
    {
        //SET TRUE TO PRINT OUTPUT FOR DEMO TO STANDARD OUT
        boolean printOutput = false;

        //Build an immutable regular time-series of single-valued pairs
        TimeSeriesOfSingleValuedPairsBuilder builder =
                dataFactory.ofTimeSeriesOfSingleValuedPairsBuilder();
        //Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( "1985-01-01T00:00:00Z" );
        List<Event<PairOfDoubles>> firstValues = new ArrayList<>();
        //Add some values
        firstValues.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), dataFactory.pairOf( 1, 2 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T12:00:00Z" ), dataFactory.pairOf( 3, 4 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T18:00:00Z" ), dataFactory.pairOf( 5, 6 ) ) );

        //Create some default metadata for the time-series
        Metadata metaData = dataFactory.getMetadataFactory().getMetadata();
        //Build the atomic time-series
        TimeSeriesOfSingleValuedPairs timeSeries =
                (TimeSeriesOfSingleValuedPairs) builder.addTimeSeriesData( firstId, firstValues )
                                                       .setMetadata( metaData )
                                                       .build();
        //Take a look at the time-series

        //Start by simply printing the untimed values using the PairedInput API, since the 
        //TimeSeries is a SingleValuedPairs
        for ( PairOfDoubles next : timeSeries )
        {
            if ( printOutput )
            {
                System.out.println( next );
            }
        }
//        1.0,2.0
//        3.0,4.0
//        5.0,6.0

        //Next, print the time-series of values by valid time in ISO-8601
        if ( printOutput )
        {
            System.out.println( timeSeries );
        }
//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-01T18:00:00Z,5.0,6.0
        //Print the basis times associated with the time-series in ISO-8601
        if ( printOutput )
        {
            System.out.println( timeSeries.getBasisTimes() );
        }
//[1985-01-01T00:00:00Z]
        //Print the difference between the basis times and valid times, aka "lead times" for a forecast dataset
        //in ISO-8601. P qualifies a timespan, T separates date and time, H represents hour.
        if ( printOutput )
        {
            System.out.println( timeSeries.getDurations() );
        }
//[PT6H, PT12H, PT18H]      

        //And the regular timestep
        if ( printOutput )
        {
            System.out.println( timeSeries.getRegularDuration() );
        }
//PT6H        
        //Add another atomic time-series to the builder and rebuild
        Instant secondId = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<PairOfDoubles>> secondValues = new ArrayList<>();
        //Add some values
        secondValues.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), dataFactory.pairOf( 7, 8 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T12:00:00Z" ), dataFactory.pairOf( 9, 10 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T18:00:00Z" ), dataFactory.pairOf( 11, 12 ) ) );
        //Build the atomic time-series
        timeSeries = builder.addTimeSeriesData( secondId, secondValues ).build();

        //Print the values by valid time
        if ( printOutput )
        {
            System.out.println( timeSeries );
        }
//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-01T18:00:00Z,5.0,6.0
//1985-01-02T06:00:00Z,7.0,8.0
//1985-01-02T12:00:00Z,9.0,10.0
//1985-01-02T18:00:00Z,11.0,12.0     

        //Iterate the atomic time-series unconditionally
        for ( Event<PairOfDoubles> next : timeSeries.timeIterator() )
        {
            if ( printOutput )
            {
                System.out.println( next + System.lineSeparator() );
            }
        }

//1985-01-01T06:00:00Z,1.0,2.0
//
//1985-01-01T12:00:00Z,3.0,4.0
//
//1985-01-01T18:00:00Z,5.0,6.0
//
//1985-01-02T06:00:00Z,7.0,8.0
//
//1985-01-02T12:00:00Z,9.0,10.0
//
//1985-01-02T18:00:00Z,11.0,12.0        

        //Iterate the atomic time-series by issue date/time
        for ( TimeSeries<PairOfDoubles> next : timeSeries.basisTimeIterator() )
        {
            if ( printOutput )
            {
                System.out.println( next + System.lineSeparator() );
            }
        }
//1985-01-01T06:00:00Z,1.0,2.0
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-01T18:00:00Z,5.0,6.0
//
//1985-01-02T06:00:00Z,7.0,8.0
//1985-01-02T12:00:00Z,9.0,10.0
//1985-01-02T18:00:00Z,11.0,12.0

        //Iterate the atomic time-series by duration
        for ( TimeSeries<PairOfDoubles> next : timeSeries.durationIterator() )
        {
            if ( printOutput )
            {
                System.out.println( next + System.lineSeparator() );
            }
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
                slicer.filterByBasisTime( timeSeries, a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) );
        if ( printOutput )
        {
            System.out.println( filteredOne );
        }

//1985-01-02T06:00:00Z,7.0,8.0
//1985-01-02T12:00:00Z,9.0,10.0
//1985-01-02T18:00:00Z,11.0,12.0        

        //Slice the time-series to obtain the atomic time-series with a duration of 12 hours only
        TimeSeries<PairOfDoubles> filteredTwo =
                slicer.filterByDuration( timeSeries, a -> a.equals( Duration.ofHours( 12 ) ) );
        if ( printOutput )
        {
            System.out.println( filteredTwo );
        }
//1985-01-01T12:00:00Z,3.0,4.0
//1985-01-02T12:00:00Z,9.0,10.0

        //Slice the time-series to obtain the atomic time-series with an issue time of 1985-01-02T00:00:00Z 
        //and a duration of 12 hours (i.e. filter chaining)
        TimeSeriesOfSingleValuedPairs filteredThree =
                slicer.filterByBasisTime( timeSeries, a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) );
        filteredThree = slicer.filterByDuration( filteredThree, b -> b.equals( Duration.ofHours( 12 ) ) );
        if ( printOutput )
        {
            System.out.println( filteredThree );
        }
//1985-01-02T12:00:00Z,9.0,10.0

        //Compute a verification metric for the TimeSeries, recalling that the TimeSeries is a SingleValuedPairs 
        MetricFactory metFac = MetricFactory.getInstance( dataFactory );
        MeanError me = metFac.ofMeanError();

        //Compute the mean error by duration
        for ( TimeSeries<PairOfDoubles> next : timeSeries.durationIterator() )
        {
            if ( printOutput )
            {
                System.out.println( me.apply( (SingleValuedPairs) next ) );
            }
        }
//1.0
//1.0
//1.0        

        //Build a regular time-series of ensemble pairs and filter to include only a trace index of 0 or 3
        //Build a time-series with three basis times 
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();
        TimeSeriesOfEnsemblePairsBuilder b = dataFactory.ofTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ),
                             metIn.pairOf( 1, new double[] { 1, 2, 3, 4, 5 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ),
                             metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ),
                             metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-03T06:00:00Z" ),
                              metIn.pairOf( 4, new double[] { 6, 7, 8, 9, 10 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-04T06:00:00Z" ),
                              metIn.pairOf( 5, new double[] { 6, 7, 8, 9, 10 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-05T06:00:00Z" ),
                              metIn.pairOf( 6, new double[] { 6, 7, 8, 9, 10 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ),
                             metIn.pairOf( 7, new double[] { 11, 12, 13, 14, 15 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ),
                             metIn.pairOf( 8, new double[] { 11, 12, 13, 14, 15 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ),
                             metIn.pairOf( 9, new double[] { 11, 12, 13, 14, 15 } ) ) );
        
        //Build some metadata
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Metadata meta = metaFac.getMetadata();
        
        //Build the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .setMetadata( meta )
                                             .build();
        
        //Iterate and test
        TimeSeriesOfEnsemblePairs regular = slicer.filterByTraceIndex( ts, q -> q.equals( 0 )
                                                                                || q.equals( 3 ) );
        //Print the filtered output by basis time
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : regular.basisTimeIterator() )
        {
            if ( printOutput )
            {
                System.out.println( next + System.lineSeparator() );
            }
        }

    }

}

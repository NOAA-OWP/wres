package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;

import org.junit.Test;

import wres.datamodel.SafeTimeSeriesOfSingleValuedPairs.SafeTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.builders.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link SafeTimeSeriesOfSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public final class SafeTimeSeriesOfSingleValuedPairsTest
{

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#basisTimeIterator()} method.
     */

    @Test
    public void test1BasisTimeIterator()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        final Metadata meta = metaFac.getMetadata();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        assertTrue( "Expected a time-series container with multiple basis times.", ts.hasMultipleTimeSeries() );
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubles> next : ts.basisTimeIterator() )
        {
            for ( Event<PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in basis-time iteration of time-series.",
                            nextPair.getValue().equals( metIn.pairOf( nextValue, nextValue ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#durationIterator()} method.
     */

    @Test
    public void test2DurationIterator()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .addTimeSeriesDataForBaseline( firstBasisTime, first )
                                                 .setMetadata( meta )
                                                 .setMetadataForBaseline( meta )
                                                 .build();

        assertTrue( "Expected a regular time-series.", ts.isRegular() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubles> next : ts.durationIterator() )
        {
            for ( Event<PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getValue().equals( metIn.pairOf( nextValue, nextValue ) ) );
            }
            //Three time-series
            assertTrue( "Unexpected number of time-series in dataset.",
                        next.getBasisTimes().size() == 3 );
            nextValue++;
        }

        //Check the regular duration of a time-series with one duration
        List<Event<PairOfDoubles>> fourth = new ArrayList<>();
        fourth.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        TimeSeriesOfSingleValuedPairs durationCheck =
                (TimeSeriesOfSingleValuedPairs) new SafeTimeSeriesOfSingleValuedPairsBuilder().addTimeSeriesData( firstBasisTime,
                                                                                                                  fourth )
                                                                                              .setMetadata( meta )
                                                                                              .build();
        assertTrue( "Unexpected regular duration for the regular time-series ",
                    Duration.ofHours( 51 ).equals( durationCheck.getRegularDuration() ) );
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#getBaselineData()} method.
     */

    @Test
    public void test3GetBaselineData()
    {
        //Build a time-series with two basis times
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );
        //Check dataset dimensions
        assertTrue( "Unexpected baseline associated with time-series.",
                    Objects.isNull( b.build().getBaselineData() ) );

        b.addTimeSeriesDataForBaseline( basisTime, values );
        b.setMetadataForBaseline( meta );

        TimeSeriesOfSingleValuedPairs baseline = b.build().getBaselineData();

        //Check dataset dimensions
        assertTrue( "Expected a time-series with one basis time and three lead times.",
                    baseline.getDurations().size() == 3 && baseline.getBasisTimes().size() == 1 );

        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubles> next : baseline.durationIterator() )
        {
            for ( Event<PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                            nextPair.getValue().equals( metIn.pairOf( nextValue, nextValue ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the addition of several time-series with a common basis time.
     */

    @Test
    public void test4AddMultipleTimeSeriesWithSameBasisTime()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();

        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Metadata meta = metaFac.getMetadata();
        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3 } );
        b.addTimeSeriesData( basisTime, first )
         .addTimeSeriesDataForBaseline( basisTime, first )
         .setMetadata( meta )
         .setMetadataForBaseline( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();

        //Add the first time-series and then append a second and third
        SafeTimeSeriesOfSingleValuedPairsBuilder c = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( "Failed to perserve climatology when building new time-series.",
                    climatology.equals( c.build().getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by basis time.",
                    climatology.equals( ( (TimeSeriesOfSingleValuedPairs) c.build()
                                                                           .durationIterator()
                                                                           .iterator()
                                                                           .next() ).getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by duration.",
                    climatology.equals( ( (TimeSeriesOfSingleValuedPairs) c.build()
                                                                           .durationIterator()
                                                                           .iterator()
                                                                           .next() ).getClimatology() ) );

        second.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        c.addTimeSeriesData( basisTime, second )
         .addTimeSeriesData( basisTime, third )
         .addTimeSeriesDataForBaseline( basisTime, second )
         .addTimeSeriesDataForBaseline( basisTime, third );

        TimeSeriesOfSingleValuedPairs tsAppend = c.build();

        //Check dataset dimensions
        assertTrue( "Expected a time-series with three basis times and three lead times.",
                    tsAppend.getDurations().size() == 3 && tsAppend.getBasisTimes().size() == 3 );
        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( Event<PairOfDoubles> nextPair : tsAppend.timeIterator() )
        {
            assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                        nextPair.getValue().equals( metIn.pairOf( nextValue, nextValue ) ) );
            nextValue++;
        }
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#filterByBasisTime(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test5FilterByBasisTime()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        //Iterate and test
        TimeSeries<PairOfDoubles> filtered = ts.filterByBasisTime( a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().get( 0 ).equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getValue().equals( metIn.pairOf( 4, 4 ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering basis times.",
                    Objects.isNull( ts.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) ) ) );

    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#filterByDuration(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test6FilterByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        //Iterate and test
        TimeSeries<PairOfDoubles> filtered =
                ts.filterByBasisTime( p -> p.equals( secondBasisTime ) )
                  .filterByDuration( q -> q.equals( Duration.ofHours( 3 ) ) );
        assertTrue( "Unexpected number of durations in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofHours( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getValue().equals( metIn.pairOf( 6, 6 ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering durations.",
                    Objects.isNull( ts.filterByDuration( p -> p.equals( Duration.ofHours( 4 ) ) ) ) );

    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void test7Exceptions()
    {
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        final Metadata meta = metaFac.getMetadata();

        //Check for exceptions on the iterators
        SafeTimeSeriesOfSingleValuedPairsBuilder d = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addTimeSeriesData( firstBasisTime, first )
                                                 .setMetadata( meta )
                                                 .build();
        try
        {
            Iterator<TimeSeries<PairOfDoubles>> it = ts.basisTimeIterator().iterator();
            it.forEachRemaining( a -> a.equals( null ) );
            it.next();
            fail( "Expected a checked exception on iterating a time-series with no more basis times left." );
        }
        catch ( NoSuchElementException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubles>> it = ts.durationIterator().iterator();
            it.forEachRemaining( a -> a.equals( null ) );
            it.next();
            fail( "Expected a checked exception on iterating a time-series with no more durations left." );
        }
        catch ( NoSuchElementException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubles>> it = ts.basisTimeIterator().iterator();
            it.next();
            it.remove();
            fail( "Expected a checked exception on attempting to remove a basis time from an immutable time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubles>> it = ts.durationIterator().iterator();
            it.next();
            it.remove();
            fail( "Expected a checked exception on attempting to remove a duration from an immutable time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
        //Check for null filters
        try
        {
            ts.filterByBasisTime( null );
            fail( "Expected a checked exception on attempting to filter by basis times with a null filter." );
        }
        catch ( NullPointerException e )
        {
        }
        try
        {
            ts.filterByDuration( null );
            fail( "Expected a checked exception on attempting to filter by duration with a null filter." );
        }
        catch ( NullPointerException e )
        {
        }
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#toString()} method.
     */

    @Test
    public void test8ToString()
    {
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        Metadata meta = metaFac.getMetadata();
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            values.add( Event.of( Instant.parse( "1985-01-01T" + String.format( "%02d", i ) + ":00:00Z" ),
                                  metIn.pairOf( 1, 1 ) ) );
            joiner.add( "(1985-01-01T" + String.format( "%02d", i ) + ":00:00Z" + "," + "1.0,1.0)" );
        }
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<PairOfDoubles>> otherValues = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            otherValues.add( Event.of( Instant.parse( "1985-01-02T" + String.format( "%02d", i ) + ":00:00Z" ),
                                       metIn.pairOf( 1, 1 ) ) );
            joiner.add( "(1985-01-02T" + String.format( "%02d", i ) + ":00:00Z" + "," + "1.0,1.0)" );
        }
        b.addTimeSeriesData( nextBasisTime, otherValues );
        assertTrue( "Unexpected string representation of compound time-series.",
                    joiner.toString().equals( b.build().toString() ) );

        //Check for equality of string representations when building in two different ways
        List<Event<List<Event<PairOfDoubles>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        input.add( Event.of( nextBasisTime, otherValues ) );
        SafeTimeSeriesOfSingleValuedPairsBuilder a = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        TimeSeriesOfSingleValuedPairs pairs =
                ( (TimeSeriesOfSingleValuedPairsBuilder) a.addTimeSeriesData( input ).setMetadata( meta ) ).build();
        assertTrue( "Unequal string representation of two time-series that should have an equal representation.",
                    joiner.toString().equals( pairs.toString() ) );
    }

    /**
     * Constructs and iterates an irregular time-series.
     */

    @Test
    public void test9IterateIrregularTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        List<Event<PairOfDoubles>> fourth = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T04:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T08:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T09:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        Instant fourthBasisTime = Instant.parse( "1985-01-04T00:00:00Z" );
        fourth.add( Event.of( Instant.parse( "1985-01-04T02:00:00Z" ), metIn.pairOf( 10, 10 ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T04:00:00Z" ), metIn.pairOf( 11, 11 ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T06:00:00Z" ), metIn.pairOf( 12, 12 ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .addTimeSeriesData( fourthBasisTime, fourth )
                                                 .setMetadata( meta )
                                                 .build();

        assertFalse( "Expected an irregular time-series.", ts.isRegular() );

        //Iterate and test
        double[] expectedOrder = new double[] { 1, 7, 4, 10, 5, 11, 6, 12, 2, 8, 3, 9 };
        int nextIndex = 0;
        for ( TimeSeries<PairOfDoubles> next : ts.durationIterator() )
        {
            for ( Event<PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getValue()
                                    .equals( metIn.pairOf( expectedOrder[nextIndex], expectedOrder[nextIndex] ) ) );
                nextIndex++;
            }
        }
    }

    /**
     * Checks that the climatology is preserved when building new time-series from existing time-series.
     */

    @Test
    public void test10ClimatologyIsPreserved()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        List<Event<PairOfDoubles>> first = new ArrayList<>();

        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Metadata meta = metaFac.getMetadata();
        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3 } );
        b.addTimeSeriesData( basisTime, first )
         .setMetadata( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();

        //Add the first time-series and then append a second and third
        SafeTimeSeriesOfSingleValuedPairsBuilder c = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( "Failed to perserve climatology when building new time-series.",
                    climatology.equals( c.build().getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by basis time.",
                    climatology.equals( ( (TimeSeriesOfSingleValuedPairs) c.build()
                                                                           .durationIterator()
                                                                           .iterator()
                                                                           .next() ).getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by duration.",
                    climatology.equals( ( (TimeSeriesOfSingleValuedPairs) c.build()
                                                                           .durationIterator()
                                                                           .iterator()
                                                                           .next() ).getClimatology() ) );
    }
    
}

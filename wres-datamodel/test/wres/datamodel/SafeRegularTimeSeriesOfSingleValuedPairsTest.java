package wres.datamodel;

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

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.SafeRegularTimeSeriesOfSingleValuedPairs.SafeRegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public final class SafeRegularTimeSeriesOfSingleValuedPairsTest
{


    /**
     * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs#basisTimeIterator()} method.
     */

    @Test
    public void test1BasisTimeIterator()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        List<PairOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        first.add( metIn.pairOf( 2, 2 ) );
        first.add( metIn.pairOf( 3, 3 ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, 4 ) );
        second.add( metIn.pairOf( 5, 5 ) );
        second.add( metIn.pairOf( 6, 6 ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, 7 ) );
        third.add( metIn.pairOf( 8, 8 ) );
        third.add( metIn.pairOf( 9, 9 ) );
        final Metadata meta = metaFac.getMetadata();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addData( firstBasisTime, first )
                                                 .addData( secondBasisTime, second )
                                                 .addData( thirdBasisTime, third )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();

        assertTrue( "Expected a time-series container with multiple basis times.", ts.hasMultipleTimeSeries() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubles> next : ts.basisTimeIterator() )
        {
            for ( Pair<Instant, PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in basis-time iteration of time-series.",
                            nextPair.getRight().equals( metIn.pairOf( nextValue, nextValue ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs#durationIterator()} method.
     */

    @Test
    public void test2DurationIterator()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        List<PairOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        first.add( metIn.pairOf( 2, 2 ) );
        first.add( metIn.pairOf( 3, 3 ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 1, 1 ) );
        second.add( metIn.pairOf( 2, 2 ) );
        second.add( metIn.pairOf( 3, 3 ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 1, 1 ) );
        third.add( metIn.pairOf( 2, 2 ) );
        third.add( metIn.pairOf( 3, 3 ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addData( firstBasisTime, first )
                                                 .addData( secondBasisTime, second )
                                                 .addData( thirdBasisTime, third )
                                                 .addDataForBaseline( firstBasisTime, first )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .setMetadataForBaseline( meta )
                                                 .build();

        assertTrue( "Expected a regular time-series for iteration.", ts.isRegular() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubles> next : ts.durationIterator() )
        {
            for ( Pair<Instant, PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getRight().equals( metIn.pairOf( nextValue, nextValue ) ) );
            }
            //Three time-series for main, one for baseline.
            assertTrue( "Unexpected number of time-series in dataset.",
                        next.getBasisTimes().size() == 3 );
            assertTrue( "Unexpected number of time-series in baseline dataset.",
                        ( (TimeSeriesOfSingleValuedPairs) next ).getDataForBaseline().size() == 1 );
            nextValue++;
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs#getBaselineData()} method.
     */

    @Test
    public void test3GetBaselineData()
    {
        //Build a time-series with two basis times
        List<PairOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( metIn.pairOf( 1, 1 ) );
        values.add( metIn.pairOf( 2, 2 ) );
        values.add( metIn.pairOf( 3, 3 ) );
        Metadata meta = metaFac.getMetadata();
        b.addData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        //Check dataset dimensions
        assertTrue( "Unexpected baseline associated with time-series.",
                    Objects.isNull( b.build().getBaselineData() ) );

        b.addDataForBaseline( basisTime, values );
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
            for ( Pair<Instant, PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                            nextPair.getRight().equals( metIn.pairOf( nextValue, nextValue ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the appending together of time-series.
     */

    @Test
    public void test4AppendTimeSeries()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        List<PairOfDoubles> third = new ArrayList<>();

        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        first.add( metIn.pairOf( 2, 2 ) );
        first.add( metIn.pairOf( 3, 3 ) );
        Metadata meta = metaFac.getMetadata();
        b.addData( basisTime, first )
         .addDataForBaseline( basisTime, first )
         .setTimeStep( Duration.ofDays( 1 ) )
         .setMetadata( meta )
         .setMetadataForBaseline( meta );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();
        //Add the first time-series and then append a second and third
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder c = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );
        second.add( metIn.pairOf( 4, 4 ) );
        second.add( metIn.pairOf( 5, 5 ) );
        second.add( metIn.pairOf( 6, 6 ) );
        third.add( metIn.pairOf( 7, 7 ) );
        third.add( metIn.pairOf( 8, 8 ) );
        third.add( metIn.pairOf( 9, 9 ) );
        c.addData( basisTime, second ).addData( basisTime, third );
        c.addDataForBaseline( basisTime, second ).addDataForBaseline( basisTime, third );

        TimeSeriesOfSingleValuedPairs tsAppend = c.build();
        //Check dataset dimensions
        assertTrue( "Expected a time-series with one basis time and three lead times.",
                    tsAppend.getDurations().size() == 9 && tsAppend.getBasisTimes().size() == 1 );
        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( Pair<Instant, PairOfDoubles> nextPair : tsAppend.timeIterator() )
        {
            assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                        nextPair.getRight().equals( metIn.pairOf( nextValue, nextValue ) ) );
            nextValue++;
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs#filterByBasisTime(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test5FilterByBasisTime()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        List<PairOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        first.add( metIn.pairOf( 2, 2 ) );
        first.add( metIn.pairOf( 3, 3 ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, 4 ) );
        second.add( metIn.pairOf( 5, 5 ) );
        second.add( metIn.pairOf( 6, 6 ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, 7 ) );
        third.add( metIn.pairOf( 8, 8 ) );
        third.add( metIn.pairOf( 9, 9 ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addData( firstBasisTime, first )
                                                 .addData( secondBasisTime, second )
                                                 .addData( thirdBasisTime, third )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();
        //Iterate and test
        TimeSeries<PairOfDoubles> filtered = ts.filterByBasisTime( a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().first().equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getRight().equals( metIn.pairOf( 4, 4 ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering basis times.",
                    Objects.isNull( ts.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) ) ) );

    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs#filterByDuration(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test6FilterByDuration()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        List<PairOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        first.add( metIn.pairOf( 2, 2 ) );
        first.add( metIn.pairOf( 3, 3 ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, 4 ) );
        second.add( metIn.pairOf( 5, 5 ) );
        second.add( metIn.pairOf( 6, 6 ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, 7 ) );
        third.add( metIn.pairOf( 8, 8 ) );
        third.add( metIn.pairOf( 9, 9 ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addData( firstBasisTime, first )
                                                 .addData( secondBasisTime, second )
                                                 .addData( thirdBasisTime, third )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();
        //Iterate and test
        TimeSeries<PairOfDoubles> filtered =
                ts.filterByBasisTime( p -> p.equals( secondBasisTime ) )
                  .filterByDuration( q -> q.equals( Duration.ofDays( 3 ) ) );
        assertTrue( "Unexpected number of durations in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofDays( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getRight().equals( metIn.pairOf( 6, 6 ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering durations.",
                    Objects.isNull( ts.filterByDuration( p -> p.equals( Duration.ofDays( 4 ) ) ) ) );

    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void test7Exceptions()
    {
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder c = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        first.add( metIn.pairOf( 2, 2 ) );
        first.add( metIn.pairOf( 3, 3 ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, 4 ) );
        second.add( metIn.pairOf( 5, 5 ) );
        second.add( metIn.pairOf( 6, 6 ) );
        final Metadata meta = metaFac.getMetadata();
        c.addData( firstBasisTime, first ).setTimeStep( Duration.ofDays( 2 ) ).setMetadata( meta );

        //Check for exceptions on the iterators
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder d = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addData( firstBasisTime, first )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();
        try
        {
            b.addData( firstBasisTime, first )
             .addData( secondBasisTime, second )
             .setTimeStep( Duration.ofDays( 1 ) )
             .addTimeSeries( c.build() )
             .setMetadata( meta )
             .build();
            fail( "Expected a checked exception on building a regular time-series with inconsistent durations." );
        }
        catch ( MetricInputException e )
        {
        }
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
            Iterator<Pair<Instant, PairOfDoubles>> it = ts.timeIterator().iterator();
            it.forEachRemaining( a -> a.equals( null ) );
            it.next();
            fail( "Expected a checked exception on iterating a time-series with no more elements left." );
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
        //Attempt to build an irregular time-series via filtering
        try
        {
            List<PairOfDoubles> values = new ArrayList<>();
            SafeRegularTimeSeriesOfSingleValuedPairsBuilder e = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
            Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
            values.add( metIn.pairOf( 1, 1 ) );
            values.add( metIn.pairOf( 2, 2 ) );
            values.add( metIn.pairOf( 3, 3 ) );
            values.add( metIn.pairOf( 4, 4 ) );
            values.add( metIn.pairOf( 5, 5 ) );
            e.addData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
            e.build().filterByDuration( a -> a.equals( Duration.ofDays( 2 ) ) || a.equals( Duration.ofDays( 5 ) ) );
            fail( "Expected a checked exception on attempting to build an irregular time-series via a filter." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
    }


    /**
     * Tests the {@link SafeRegularTimeSeriesOfSingleValuedPairs#toString()} method.
     */

    @Test
    public void test8ToString()
    {
        List<PairOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        for ( int i = 0; i < 5; i++ )
        {
            values.add( metIn.pairOf( 1, 1 ) );
        }
        Metadata meta = metaFac.getMetadata();
        b.addData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            joiner.add( "1985-01-0" + ( i + 2 ) + "T00:00:00Z" + "," + "1.0,1.0" );
        }

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addData( nextBasisTime, values );
        for ( int i = 0; i < 5; i++ )
        {
            joiner.add( "1985-01-0" + ( i + 3 ) + "T00:00:00Z" + "," + "1.0,1.0" );
        }
        assertTrue( "Unexpected string representation of compound time-series.",
                    joiner.toString().equals( b.build().toString() ) );
    }

}

package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.SafeRegularTimeSeriesOfSingleValuedPairs.SafeRegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link SafeRegularTimeSeriesOfPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public final class SafeRegularTimeSeriesOfPairsTest
{

    /**
     * Tests for regularity.
     */

    @Test
    public void test1IsRegular()
    {
        //Build a time-series with one basis time
        List<PairOfDoubles> first = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        final Metadata meta = metaFac.getMetadata();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();
        assertTrue( "Expected a regular time-series.", ts.isRegular() );
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfPairs#getRegularDuration()} method.
     */

    @Test
    public void test2GetRegularDuration()
    {
        //Build a time-series with one basis time
        List<PairOfDoubles> first = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, 1 ) );
        final Metadata meta = metaFac.getMetadata();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();
        Duration benchmark = Duration.ofDays( 1 );
        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    ts.getRegularDuration().equals( benchmark ) );
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfPairs#hasMultipleTimeSeries()} method.
     */

    @Test
    public void test3HasMultipleTimeSeries()
    {
        //Build a time-series with one basis time
        List<PairOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( metIn.pairOf( 1, 1 ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );

        //Check dataset count
        assertTrue( "Expected a time-series with one basis time.", !b.build().hasMultipleTimeSeries() );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        assertTrue( "Expected a time-series with multiple basis times.", b.build().hasMultipleTimeSeries() );
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfPairs#getEarliestBasisTime()} method.
     */

    @Test
    public void test4GetEarliestBasisTime()
    {
        //Build a time-series with two basis times
        List<PairOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( metIn.pairOf( 1, 1 ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        Instant benchmark = Instant.parse( "1985-01-01T00:00:00Z" );
        assertTrue( "The earliest basis time does not match the benchmark.",
                    b.build().getEarliestBasisTime().equals( benchmark ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        assertTrue( "The earliest basis time does not match the benchmark.",
                    b.build().getEarliestBasisTime().equals( benchmark ) );
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfPairs#getBasisTimes()}.
     */

    @Test
    public void test5GetBasisTimes()
    {
        //Build a time-series with two basis times
        List<PairOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder b = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( metIn.pairOf( 1, 1 ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values )
         .setTimeStep( Duration.ofDays( 1 ) )
         .setMetadata( meta );
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        SafeRegularTimeSeriesOfSingleValuedPairs pairs = b.build();
        //Check dataset count
        assertTrue( "Expected a time-series with two basis times.", pairs.getBasisTimes().size() == 2 );
        //Check the basis times
        assertTrue( "First basis time missing from time-series.",
                    pairs.getBasisTimes().get( 0 ).equals( basisTime ) );
        Iterator<Instant> it = pairs.getBasisTimes().iterator();
        it.next();
        assertTrue( "Second basis time missing from time-series.", it.next().equals( nextBasisTime ) );
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfPairs#getDurations()} method.
     */

    @Test
    public void test6GetDurations()
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
        b.addTimeSeriesData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        //Check dataset count
        assertTrue( "Expected a time-series with three lead times.", b.build().getDurations().size() == 3 );
        //Check the lead times
        assertTrue( "First lead time missing from time-series.",
                    b.build().getDurations().contains( Duration.ofDays( 1 ) ) );
        assertTrue( "Second lead time missing from time-series.",
                    b.build().getDurations().contains( Duration.ofDays( 2 ) ) );
        assertTrue( "Third lead time missing from time-series.",
                    b.build().getDurations().contains( Duration.ofDays( 3 ) ) );
    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void test7Exceptions()
    {
        List<PairOfDoubles> first = new ArrayList<>();
        List<PairOfDoubles> second = new ArrayList<>();
        List<PairOfDoubles> third = new ArrayList<>();
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
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, 7 ) );

        final Metadata meta = metaFac.getMetadata();
        c.addTimeSeriesData( firstBasisTime, first ).setTimeStep( Duration.ofDays( 2 ) ).setMetadata( meta );
        try
        {
            b.addTimeSeriesData( firstBasisTime, first )
             .addTimeSeriesData( secondBasisTime, second )
             .setTimeStep( Duration.ofDays( 1 ) )
             .addTimeSeries( c.build() )
             .setMetadata( meta )
             .build();
            fail( "Expected a checked exception on building a regular time-series with inconsistent durations." );
        }
        catch ( MetricInputException e )
        {
        }
        //Check for null timestep
        c.setTimeStep( null );
        try
        {
            c.build();
            fail( "Expected a checked exception on building a regular time-series without a timestep." );
        }
        catch ( MetricInputException e )
        {
        }
        //Check for inconsistent forecast horizons
        try
        {
            b.addTimeSeriesData( thirdBasisTime, third ).setMetadata( meta ).build();
            fail( "Expected a checked exception on building a regular time-series with an inconsistent time horizon." );
        }
        catch ( MetricInputException e )
        {
        }
        //Check for inconsistent forecast horizons in the baseline
        try
        {
            c.addTimeSeriesDataForBaseline( firstBasisTime, first )
             .addTimeSeriesDataForBaseline( thirdBasisTime, third )
             .setTimeStep( Duration.ofDays( 1 ) )
             .setMetadata( meta )
             .setMetadataForBaseline( meta )
             .build();
            fail( "Expected a checked exception on building a regular time-series with an inconsistent baseline time "
                  + "horizon." );
        }
        catch ( MetricInputException e )
        {
        }
        //Check for a baseline that is inconsistent with the main forecast
        try
        {
            new SafeRegularTimeSeriesOfSingleValuedPairsBuilder().addTimeSeriesData( firstBasisTime, first )
                                                                 .addTimeSeriesDataForBaseline( thirdBasisTime, third )
                                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                                 .setMetadata( meta )
                                                                 .setMetadataForBaseline( meta )
                                                                 .build();
            fail( "Expected a checked exception on building a regular time-series with an inconsistent baseline." );
        }
        catch ( MetricInputException e )
        {
        }

        //Check for exceptions on the iterators
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder d = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addTimeSeriesData( firstBasisTime, first )
                                                 .setTimeStep( Duration.ofDays( 1 ) )
                                                 .setMetadata( meta )
                                                 .build();
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
            Iterator<Pair<Instant, PairOfDoubles>> it = ts.timeIterator().iterator();
            it.next();
            it.remove();
            fail( "Expected a checked exception on attempting to remove an element from an immutable time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
        //Try to mutate pair
        try
        {
            Iterator<Pair<Instant, PairOfDoubles>> it = ts.timeIterator().iterator();
            it.next().setValue( metIn.pairOf( 0, 1 ) );
            it.remove();
            fail( "Expected a checked exception on attempting to modify a pair in an immutable time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfPairs#toString()} method.
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
        b.addTimeSeriesData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            joiner.add( "(1985-01-0" + ( i + 2 ) + "T00:00:00Z" + "," + "1.0,1.0)" );
        }

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        for ( int i = 0; i < 5; i++ )
        {
            joiner.add( "(1985-01-0" + ( i + 3 ) + "T00:00:00Z" + "," + "1.0,1.0)" );
        }
        assertTrue( "Unexpected string representation of compound time-series.",
                    joiner.toString().equals( b.build().toString() ) );
    }

}

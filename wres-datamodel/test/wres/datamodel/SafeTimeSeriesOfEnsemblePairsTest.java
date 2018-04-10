package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.SafeTimeSeriesOfEnsemblePairs.SafeTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.builders.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link SafeTimeSeriesOfEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public final class SafeTimeSeriesOfEnsemblePairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link SafeTimeSeriesOfEnsemblePairs#basisTimeIterator()} method.
     */

    @Test
    public void testBasisTimeIterator()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, new double[] { 9 } ) ) );
        final Metadata meta = metaFac.getMetadata();
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .setMetadata( meta )
                                             .build();
        assertTrue( "Expected a time-series container with multiple basis times.", ts.hasMultipleTimeSeries() );
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : ts.basisTimeIterator() )
        {
            for ( Event<PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in basis-time iteration of time-series.",
                            nextPair.getValue().equals( metIn.pairOf( nextValue, new double[] { nextValue } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the {@link SafeTimeSeriesOfEnsemblePairs#durationIterator()} method.
     */

    @Test
    public void testDurationIterator()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .addTimeSeriesDataForBaseline( firstBasisTime, first )
                                             .setMetadata( meta )
                                             .setMetadataForBaseline( meta )
                                             .build();

        assertTrue( "Expected a regular time-series.", ts.isRegular() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : ts.durationIterator() )
        {
            for ( Event<PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getValue().equals( metIn.pairOf( nextValue, new double[] { nextValue } ) ) );
            }
            //Three time-series
            assertTrue( "Unexpected number of time-series in dataset.",
                        next.getBasisTimes().size() == 3 );
            nextValue++;
        }

        //Check the regular duration of a time-series with one duration
        List<Event<PairOfDoubleAndVectorOfDoubles>> fourth = new ArrayList<>();
        fourth.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        TimeSeriesOfEnsemblePairs durationCheck =
                (TimeSeriesOfEnsemblePairs) new SafeTimeSeriesOfEnsemblePairsBuilder().addTimeSeriesData( firstBasisTime,
                                                                                                          fourth )
                                                                                      .setMetadata( meta )
                                                                                      .build();
        assertTrue( "Unexpected regular duration for the regular time-series ",
                    Duration.ofHours( 51 ).equals( durationCheck.getRegularDuration() ) );
    }

    /**
     * Tests the {@link SafeTimeSeriesOfEnsemblePairs#getBaselineData()} method.
     */

    @Test
    public void testGetBaselineData()
    {
        //Build a time-series with two basis times
        List<Event<PairOfDoubleAndVectorOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        values.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        values.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );
        //Check dataset dimensions
        assertTrue( "Unexpected baseline associated with time-series.",
                    Objects.isNull( b.build().getBaselineData() ) );

        b.addTimeSeriesDataForBaseline( basisTime, values );
        b.setMetadataForBaseline( meta );

        TimeSeriesOfEnsemblePairs baseline = b.build().getBaselineData();

        //Check dataset dimensions
        assertTrue( "Expected a time-series with one basis time and three lead times.",
                    baseline.getDurations().size() == 3 && baseline.getBasisTimes().size() == 1 );

        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : baseline.durationIterator() )
        {
            for ( Event<PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                            nextPair.getValue().equals( metIn.pairOf( nextValue, new double[] { nextValue } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the addition of several time-series with a common basis time.
     */

    @Test
    public void testAddMultipleTimeSeriesWithSameBasisTime()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();

        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Metadata meta = metaFac.getMetadata();
        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3 } );
        b.addTimeSeriesData( basisTime, first )
         .addTimeSeriesDataForBaseline( basisTime, first )
         .setMetadata( meta )
         .setMetadataForBaseline( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfEnsemblePairs ts = b.build();

        //Add the first time-series and then append a second and third
        SafeTimeSeriesOfEnsemblePairsBuilder c = new SafeTimeSeriesOfEnsemblePairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( "Failed to perserve climatology when building new time-series.",
                    climatology.equals( c.build().getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by basis time.",
                    climatology.equals( ( (TimeSeriesOfEnsemblePairs) c.build()
                                                                       .durationIterator()
                                                                       .iterator()
                                                                       .next() ).getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by duration.",
                    climatology.equals( ( (TimeSeriesOfEnsemblePairs) c.build()
                                                                       .durationIterator()
                                                                       .iterator()
                                                                       .next() ).getClimatology() ) );

        second.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 6, new double[] { 6 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 9, new double[] { 9 } ) ) );
        c.addTimeSeriesData( basisTime, second )
         .addTimeSeriesData( basisTime, third )
         .addTimeSeriesDataForBaseline( basisTime, second )
         .addTimeSeriesDataForBaseline( basisTime, third );

        TimeSeriesOfEnsemblePairs tsAppend = c.build();

        //Check dataset dimensions
        assertTrue( "Expected a time-series with three basis times and three lead times.",
                    tsAppend.getDurations().size() == 3 && tsAppend.getBasisTimes().size() == 3 );
        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( Event<PairOfDoubleAndVectorOfDoubles> nextPair : tsAppend.timeIterator() )
        {
            assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                        nextPair.getValue().equals( metIn.pairOf( nextValue, new double[] { nextValue } ) ) );
            nextValue++;
        }
    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void testExceptions()
    {
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        final Metadata meta = metaFac.getMetadata();

        //Check for exceptions on the iterators
        SafeTimeSeriesOfEnsemblePairsBuilder d = new SafeTimeSeriesOfEnsemblePairsBuilder();
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) d.addTimeSeriesData( firstBasisTime, first )
                                             .setMetadata( meta )
                                             .build();

        //Iterate
        exception.expect( NoSuchElementException.class );
        Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> noneSuchBasis = ts.basisTimeIterator().iterator();
        noneSuchBasis.forEachRemaining( a -> a.equals( null ) );
        noneSuchBasis.next();

        Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> noneSuchDuration = ts.durationIterator().iterator();
        noneSuchDuration.forEachRemaining( a -> a.equals( null ) );
        noneSuchDuration.next();

        //Mutate 
        exception.expect( UnsupportedOperationException.class );

        Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> immutableBasis = ts.basisTimeIterator().iterator();
        immutableBasis.next();
        immutableBasis.remove();

        Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> immutableDuration = ts.durationIterator().iterator();
        immutableDuration.next();
        immutableDuration.remove();
    }

    /**
     * Tests the {@link SafeTimeSeriesOfEnsemblePairs#toString()} method.
     */

    @Test
    public void testToString()
    {
        List<Event<PairOfDoubleAndVectorOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        Metadata meta = metaFac.getMetadata();
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            values.add( Event.of( Instant.parse( "1985-01-01T" + String.format( "%02d", i ) + ":00:00Z" ),
                                  metIn.pairOf( 1, new double[] { 1 } ) ) );
            joiner.add( "(1985-01-01T" + String.format( "%02d", i ) + ":00:00Z" + ",key: " + "1.0 value: [1.0])" );
        }
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<PairOfDoubleAndVectorOfDoubles>> otherValues = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            otherValues.add( Event.of( Instant.parse( "1985-01-02T" + String.format( "%02d", i ) + ":00:00Z" ),
                                       metIn.pairOf( 1, new double[] { 1 } ) ) );
            joiner.add( "(1985-01-02T" + String.format( "%02d", i ) + ":00:00Z" + ",key: " + "1.0 value: [1.0])" );
        }

        b.addTimeSeriesData( nextBasisTime, otherValues );
        assertTrue( "Unexpected string representation of compound time-series.",
                    joiner.toString().equals( b.build().toString() ) );

        //Check for equality of string representations when building in two different ways
        List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        input.add( Event.of( nextBasisTime, otherValues ) );
        SafeTimeSeriesOfEnsemblePairsBuilder a = new SafeTimeSeriesOfEnsemblePairsBuilder();
        TimeSeriesOfEnsemblePairs pairs =
                ( (TimeSeriesOfEnsemblePairsBuilder) a.addTimeSeriesData( input ).setMetadata( meta ) ).build();
        assertTrue( "Unequal string representation of two time-series that should have an equal representation.",
                    joiner.toString().equals( pairs.toString() ) );
    }

    /**
     * Constructs and iterates an irregular time-series.
     */

    @Test
    public void testIterateIrregularTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> fourth = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T04:00:00Z" ), metIn.pairOf( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), metIn.pairOf( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T08:00:00Z" ), metIn.pairOf( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T09:00:00Z" ), metIn.pairOf( 9, new double[] { 9 } ) ) );
        Instant fourthBasisTime = Instant.parse( "1985-01-04T00:00:00Z" );
        fourth.add( Event.of( Instant.parse( "1985-01-04T02:00:00Z" ), metIn.pairOf( 10, new double[] { 10 } ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T04:00:00Z" ), metIn.pairOf( 11, new double[] { 11 } ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T06:00:00Z" ), metIn.pairOf( 12, new double[] { 12 } ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .addTimeSeriesData( fourthBasisTime, fourth )
                                             .setMetadata( meta )
                                             .build();

        assertFalse( "Expected an irregular time-series.", ts.isRegular() );

        //Iterate and test
        double[] expectedOrder = new double[] { 1, 7, 4, 10, 5, 11, 6, 12, 2, 8, 3, 9 };
        int nextIndex = 0;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : ts.durationIterator() )
        {
            for ( Event<PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getValue()
                                    .equals( metIn.pairOf( expectedOrder[nextIndex],
                                                           new double[] { expectedOrder[nextIndex] } ) ) );
                nextIndex++;
            }
        }
    }

    /**
     * Checks that the climatology is preserved when building new time-series from existing time-series.
     */

    @Test
    public void testClimatologyIsPreserved()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();

        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Metadata meta = metaFac.getMetadata();
        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3 } );
        b.addTimeSeriesData( basisTime, first )
         .setMetadata( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfEnsemblePairs ts = b.build();

        //Add the first time-series and then append a second and third
        SafeTimeSeriesOfEnsemblePairsBuilder c = new SafeTimeSeriesOfEnsemblePairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( "Failed to perserve climatology when building new time-series.",
                    climatology.equals( c.build().getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by basis time.",
                    climatology.equals( ( (TimeSeriesOfEnsemblePairs) c.build()
                                                                       .durationIterator()
                                                                       .iterator()
                                                                       .next() ).getClimatology() ) );
        assertTrue( "Failed to perserve climatology when iterating new time-series by duration.",
                    climatology.equals( ( (TimeSeriesOfEnsemblePairs) c.build()
                                                                       .durationIterator()
                                                                       .iterator()
                                                                       .next() ).getClimatology() ) );
    }

}

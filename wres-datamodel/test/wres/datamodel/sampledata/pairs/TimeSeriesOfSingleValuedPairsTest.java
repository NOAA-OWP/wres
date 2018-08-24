package wres.datamodel.sampledata.pairs;

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

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link SafeTimeSeriesOfSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesOfSingleValuedPairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#basisTimeIterator()} method.
     */

    @Test
    public void test1BasisTimeIterator()
    {
        //Build a time-series with three basis times 
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );
        final SampleMetadata meta = SampleMetadata.of();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        assertTrue( "Expected a time-series container with multiple basis times.", ts.hasMultipleTimeSeries() );
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<SingleValuedPair> next : ts.basisTimeIterator() )
        {
            for ( Event<SingleValuedPair> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in basis-time iteration of time-series.",
                            nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
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
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
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
        for ( TimeSeries<SingleValuedPair> next : ts.durationIterator() )
        {
            for ( Event<SingleValuedPair> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
            }
            //Three time-series
            assertTrue( "Unexpected number of time-series in dataset.",
                        next.getBasisTimes().size() == 3 );
            nextValue++;
        }

        //Check the regular duration of a time-series with one duration
        List<Event<SingleValuedPair>> fourth = new ArrayList<>();
        fourth.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        TimeSeriesOfSingleValuedPairs durationCheck =
                (TimeSeriesOfSingleValuedPairs) new TimeSeriesOfSingleValuedPairsBuilder().addTimeSeriesData( firstBasisTime,
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
        List<Event<SingleValuedPair>> values = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
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
        for ( TimeSeries<SingleValuedPair> next : baseline.durationIterator() )
        {
            for ( Event<SingleValuedPair> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                            nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
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
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();

        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        VectorOfDoubles climatology = VectorOfDoubles.of( new double[] { 1, 2, 3 } );
        b.addTimeSeriesData( basisTime, first )
         .addTimeSeriesDataForBaseline( basisTime, first )
         .setMetadata( meta )
         .setMetadataForBaseline( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();

        //Add the first time-series and then append a second and third
        TimeSeriesOfSingleValuedPairsBuilder c = new TimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );

        second.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );
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
        for ( Event<SingleValuedPair> nextPair : tsAppend.timeIterator() )
        {
            assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                        nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
            nextValue++;
        }
    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void test7Exceptions()
    {
        List<Event<SingleValuedPair>> first = new ArrayList<>();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        final SampleMetadata meta = SampleMetadata.of();

        //Check for exceptions on the iterators
        TimeSeriesOfSingleValuedPairsBuilder d = new TimeSeriesOfSingleValuedPairsBuilder();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addTimeSeriesData( firstBasisTime, first )
                                                 .setMetadata( meta )
                                                 .build();

        //Iterate
        exception.expect( NoSuchElementException.class );
        Iterator<TimeSeries<SingleValuedPair>> noneSuchBasis = ts.basisTimeIterator().iterator();
        noneSuchBasis.forEachRemaining( a -> a.equals( null ) );
        noneSuchBasis.next();

        Iterator<TimeSeries<SingleValuedPair>> noneSuchDuration = ts.durationIterator().iterator();
        noneSuchDuration.forEachRemaining( a -> a.equals( null ) );
        noneSuchDuration.next();

        //Mutate 
        exception.expect( UnsupportedOperationException.class );

        Iterator<TimeSeries<SingleValuedPair>> immutableBasis = ts.basisTimeIterator().iterator();
        immutableBasis.next();
        immutableBasis.remove();

        Iterator<TimeSeries<SingleValuedPair>> immutableDuration = ts.durationIterator().iterator();
        immutableDuration.next();
        immutableDuration.remove();
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#toString()} method.
     */

    @Test
    public void test8ToString()
    {
        List<Event<SingleValuedPair>> values = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        SampleMetadata meta = SampleMetadata.of();
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            values.add( Event.of( Instant.parse( "1985-01-01T" + String.format( "%02d", i ) + ":00:00Z" ),
                                  SingleValuedPair.of( 1, 1 ) ) );
            joiner.add( "(1985-01-01T" + String.format( "%02d", i ) + ":00:00Z" + "," + "1.0,1.0)" );
        }
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<SingleValuedPair>> otherValues = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            otherValues.add( Event.of( Instant.parse( "1985-01-02T" + String.format( "%02d", i ) + ":00:00Z" ),
                                       SingleValuedPair.of( 1, 1 ) ) );
            joiner.add( "(1985-01-02T" + String.format( "%02d", i ) + ":00:00Z" + "," + "1.0,1.0)" );
        }
        b.addTimeSeriesData( nextBasisTime, otherValues );
        assertTrue( "Unexpected string representation of compound time-series.",
                    joiner.toString().equals( b.build().toString() ) );

        //Check for equality of string representations when building in two different ways
        List<Event<List<Event<SingleValuedPair>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        input.add( Event.of( nextBasisTime, otherValues ) );
        TimeSeriesOfSingleValuedPairsBuilder a = new TimeSeriesOfSingleValuedPairsBuilder();
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
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();
        List<Event<SingleValuedPair>> fourth = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T04:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T08:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T09:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );
        Instant fourthBasisTime = Instant.parse( "1985-01-04T00:00:00Z" );
        fourth.add( Event.of( Instant.parse( "1985-01-04T02:00:00Z" ), SingleValuedPair.of( 10, 10 ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T04:00:00Z" ), SingleValuedPair.of( 11, 11 ) ) );
        fourth.add( Event.of( Instant.parse( "1985-01-04T06:00:00Z" ), SingleValuedPair.of( 12, 12 ) ) );
        SampleMetadata meta = SampleMetadata.of();
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
        for ( TimeSeries<SingleValuedPair> next : ts.durationIterator() )
        {
            for ( Event<SingleValuedPair> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getValue()
                                    .equals( SingleValuedPair.of( expectedOrder[nextIndex],
                                                                 expectedOrder[nextIndex] ) ) );
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
        List<Event<SingleValuedPair>> first = new ArrayList<>();

        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        VectorOfDoubles climatology = VectorOfDoubles.of( new double[] { 1, 2, 3 } );
        b.addTimeSeriesData( basisTime, first )
         .setMetadata( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();

        //Add the first time-series and then append a second and third
        TimeSeriesOfSingleValuedPairsBuilder c = new TimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( "Failed to perserve climatology when building new time-series.",
                    climatology.equals( c.build().getClimatology() ) );
    }

}

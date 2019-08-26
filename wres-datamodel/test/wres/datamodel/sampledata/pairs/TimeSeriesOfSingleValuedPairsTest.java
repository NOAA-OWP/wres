package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleMetadata;
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

    private static final String ZERO_ZEE = ":00:00Z";
    private static final String TWELFTH_TIME = "1985-01-03T03:00:00Z";
    private static final String ELEVENTH_TIME = "1985-01-03T02:00:00Z";
    private static final String TENTH_TIME = "1985-01-03T01:00:00Z";
    private static final String NINTH_TIME = "1985-01-03T00:00:00Z";
    private static final String EIGHTH_TIME = "1985-01-02T03:00:00Z";
    private static final String SEVENTH_TIME = "1985-01-02T02:00:00Z";
    private static final String SIXTH_TIME = "1985-01-02T01:00:00Z";
    private static final String FIFTH_TIME = "1985-01-02T00:00:00Z";
    private static final String FOURTH_TIME = "1985-01-01T03:00:00Z";
    private static final String THIRD_TIME = "1985-01-01T02:00:00Z";
    private static final String SECOND_TIME = "1985-01-01T01:00:00Z";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#referenceTimeIterator()} method.
     */

    @Test
    public void testReferenceTimeIterator()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> third = new TreeSet<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( secondBasisTime, Instant.parse( SIXTH_TIME ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( secondBasisTime, Instant.parse( SEVENTH_TIME ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( secondBasisTime, Instant.parse( EIGHTH_TIME ), SingleValuedPair.of( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( thirdBasisTime, Instant.parse( TENTH_TIME ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( thirdBasisTime, Instant.parse( ELEVENTH_TIME ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( thirdBasisTime, Instant.parse( TWELFTH_TIME ), SingleValuedPair.of( 9, 9 ) ) );

        final SampleMetadata meta = SampleMetadata.of();

        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                 first ) )
                                                 .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                 second ) )
                                                 .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                                                 third ) )
                                                 .setMetadata( meta )
                                                 .build();
        assertTrue( ts.getReferenceTimes().size() == 3 );
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<SingleValuedPair> next : ts.getTimeSeries() )
        {
            for ( Event<SingleValuedPair> nextPair : next.getEvents() )
            {
                assertTrue( nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#durationIterator()} method.
     */

    @Test
    public void testDurationIterator()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> third = new TreeSet<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( secondBasisTime, Instant.parse( SIXTH_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        second.add( Event.of( secondBasisTime, Instant.parse( SEVENTH_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        second.add( Event.of( secondBasisTime, Instant.parse( EIGHTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( thirdBasisTime, Instant.parse( TENTH_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        third.add( Event.of( thirdBasisTime, Instant.parse( ELEVENTH_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        third.add( Event.of( thirdBasisTime, Instant.parse( TWELFTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        //Add the time-series, with only one for baseline
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                 first ) )
                                                 .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                 second ) )
                                                 .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                                                 third ) )
                                                 .addTimeSeriesForBaseline( TimeSeries.of( firstBasisTime,
                                                                                            first ) )
                                                 .setMetadata( meta )
                                                 .setMetadataForBaseline( meta )
                                                 .build();

        //Iterate and test
        int nextValue = 1;

        SortedSet<Duration> durations = ts.getDurations();

        for ( Duration duration : durations )
        {
            Set<Instant> basisTimes = new HashSet<>();
            List<Event<SingleValuedPair>> events = Slicer.filterByDuration( ts, a -> a.equals( duration ) );
            for ( Event<SingleValuedPair> nextPair : events )
            {
                assertTrue( nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
                basisTimes.add( nextPair.getReferenceTime() );
            }
            //Three time-series
            assertTrue( basisTimes.size() == 3 );
            nextValue++;
        }

        //Check the regular duration of a time-series with one duration
        SortedSet<Event<SingleValuedPair>> fourth = new TreeSet<>();
        fourth.add( Event.of( firstBasisTime, Instant.parse( TWELFTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );

        TimeSeriesOfSingleValuedPairsBuilder bu = new TimeSeriesOfSingleValuedPairsBuilder();

        TimeSeriesOfSingleValuedPairs durationCheck =
                (TimeSeriesOfSingleValuedPairs) bu.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                  fourth ) )
                                                  .setMetadata( meta )
                                                  .build();
        assertTrue( Duration.ofHours( 51 ).equals( durationCheck.getDurations().first() ) );
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#getBaselineData()} method.
     */

    @Test
    public void testGetBaselineData()
    {
        //Build a time-series with two basis times
        SortedSet<Event<SingleValuedPair>> values = new TreeSet<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        values.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        values.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();

        TimeSeries<SingleValuedPair> timeSeries = TimeSeries.of( basisTime,
                                                                   values );
        b.addTimeSeries( timeSeries ).setMetadata( meta );

        //Check dataset dimensions
        assertTrue( Objects.isNull( b.build().getBaselineData() ) );

        b.addTimeSeriesForBaseline( timeSeries ).setMetadataForBaseline( meta );

        TimeSeriesOfSingleValuedPairs baseline = b.build().getBaselineData();

        //Check dataset dimensions
        assertTrue( baseline.getDurations().size() == 3 && baseline.getReferenceTimes().size() == 1 );

        //Check dataset
        //Iterate and test
        int nextValue = 1;
        SortedSet<Duration> durations = baseline.getDurations();

        for ( Duration duration : durations )
        {
            List<Event<SingleValuedPair>> events = Slicer.filterByDuration( baseline, a -> a.equals( duration ) );
            for ( Event<SingleValuedPair> nextPair : events )
            {
                assertTrue( nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
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
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> third = new TreeSet<>();

        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3 );

        TimeSeries<SingleValuedPair> firstSeries = TimeSeries.of( basisTime,
                                                                    first );

        b.addTimeSeries( firstSeries )
         .addTimeSeriesForBaseline( firstSeries )
         .setMetadata( meta )
         .setMetadataForBaseline( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();

        //Add the first time-series and then append a second and third
        TimeSeriesOfSingleValuedPairsBuilder c = new TimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );

        second.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 6, 6 ) ) );

        third.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 9, 9 ) ) );

        TimeSeries<SingleValuedPair> secondSeries = TimeSeries.of( basisTime,
                                                                     second );

        TimeSeries<SingleValuedPair> thirdSeries = TimeSeries.of( basisTime,
                                                                    third );

        c.addTimeSeries( secondSeries )
         .addTimeSeries( thirdSeries )
         .addTimeSeriesForBaseline( secondSeries )
         .addTimeSeriesForBaseline( thirdSeries );

        TimeSeriesOfSingleValuedPairs tsAppend = c.build();

        //Check dataset dimensions
        assertTrue( tsAppend.getDurations().size() == 3 && StreamSupport.stream( tsAppend.getTimeSeries()
                                                                                         .spliterator(),
                                                                                 false )
                                                                        .count() == 3 );
        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( Event<SingleValuedPair> nextPair : tsAppend.eventIterator() )
        {
            assertTrue( nextPair.getValue().equals( SingleValuedPair.of( nextValue, nextValue ) ) );
            nextValue++;
        }
    }

    /**
     * Tests for an excpected exception when attempting to iterate beyond the available data.
     */

    @Test
    public void testForNoSuchElementOnIteration()
    {
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        final SampleMetadata meta = SampleMetadata.of();

        //Check for exceptions on the iterators
        TimeSeriesOfSingleValuedPairsBuilder d = new TimeSeriesOfSingleValuedPairsBuilder();
        TimeSeries<SingleValuedPair> firstSeries = TimeSeries.of( firstBasisTime,
                                                                    first );
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addTimeSeries( firstSeries )
                                                 .setMetadata( meta )
                                                 .build();

        //Iterate
        exception.expect( NoSuchElementException.class );
        Iterator<TimeSeries<SingleValuedPair>> noneSuchBasis = ts.getTimeSeries().iterator();
        noneSuchBasis.forEachRemaining( Objects::isNull );
        noneSuchBasis.next();
    }

    /**
     * Tests for an expected exception when attempting to mutate the basis times.
     */

    @Test
    public void testBasisTimesAreImmutable()
    {
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        final SampleMetadata meta = SampleMetadata.of();

        //Check for exceptions on the iterators
        TimeSeriesOfSingleValuedPairsBuilder d = new TimeSeriesOfSingleValuedPairsBuilder();
        TimeSeries<SingleValuedPair> firstSeries = TimeSeries.of( firstBasisTime,
                                                                    first );
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addTimeSeries( firstSeries )
                                                 .setMetadata( meta )
                                                 .build();

        //Mutate 
        exception.expect( UnsupportedOperationException.class );

        Iterator<TimeSeries<SingleValuedPair>> immutableBasis = ts.getTimeSeries().iterator();
        immutableBasis.next();
        immutableBasis.remove();
    }

    /**
     * Tests the {@link SafeTimeSeriesOfSingleValuedPairs#toString()} method.
     */

    @Test
    public void testToString()
    {
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        SampleMetadata meta = SampleMetadata.of();
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            first.add( Event.of( basisTime,
                                 Instant.parse( "1985-01-01T" + String.format( "%02d", i ) + ZERO_ZEE ),
                                 SingleValuedPair.of( 1, 1 ) ) );
            joiner.add( "(" + basisTime + ",1985-01-01T" + String.format( "%02d", i ) + ZERO_ZEE + "," + "1.0,1.0)" );
        }

        TimeSeries<SingleValuedPair> firstSeries = TimeSeries.of( basisTime,
                                                                    first );

        b.addTimeSeries( firstSeries ).setMetadata( meta );

        //Check dataset count
        assertTrue( joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( FIFTH_TIME );
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            second.add( Event.of( nextBasisTime,
                                  Instant.parse( "1985-01-02T" + String.format( "%02d", i ) + ZERO_ZEE ),
                                  SingleValuedPair.of( 1, 1 ) ) );
            joiner.add( "(" + nextBasisTime
                        + ",1985-01-02T"
                        + String.format( "%02d", i )
                        + ZERO_ZEE
                        + ","
                        + "1.0,1.0)" );
        }

        TimeSeries<SingleValuedPair> secondSeries = TimeSeries.of( nextBasisTime,
                                                                     second );
        b.addTimeSeries( secondSeries );
        assertTrue( joiner.toString().equals( b.build().toString() ) );
    }

    /**
     * Constructs and iterates an irregular time-series.
     */

    @Test
    public void testIterateIrregularTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> third = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> fourth = new TreeSet<>();

        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( "1985-01-01T08:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( "1985-01-01T09:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( secondBasisTime, Instant.parse( SEVENTH_TIME ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( secondBasisTime, Instant.parse( "1985-01-02T04:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( secondBasisTime, Instant.parse( "1985-01-02T06:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( thirdBasisTime, Instant.parse( TENTH_TIME ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( thirdBasisTime, Instant.parse( "1985-01-03T08:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( thirdBasisTime, Instant.parse( "1985-01-03T09:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );
        Instant fourthBasisTime = Instant.parse( "1985-01-04T00:00:00Z" );
        fourth.add( Event.of( fourthBasisTime,
                              Instant.parse( "1985-01-04T02:00:00Z" ),
                              SingleValuedPair.of( 10, 10 ) ) );
        fourth.add( Event.of( fourthBasisTime,
                              Instant.parse( "1985-01-04T04:00:00Z" ),
                              SingleValuedPair.of( 11, 11 ) ) );
        fourth.add( Event.of( fourthBasisTime,
                              Instant.parse( "1985-01-04T06:00:00Z" ),
                              SingleValuedPair.of( 12, 12 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        //Add the time-series, with only one for baseline
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                 first ) )
                                                 .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                 second ) )
                                                 .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                                                 third ) )
                                                 .addTimeSeries( TimeSeries.of( fourthBasisTime,
                                                                                 fourth ) )
                                                 .setMetadata( meta )
                                                 .build();

        //Iterate and test
        double[] expectedOrder = new double[] { 1, 7, 4, 10, 5, 11, 6, 12, 2, 8, 3, 9 };
        int nextIndex = 0;


        SortedSet<Duration> durations = ts.getDurations();

        for ( Duration nextDuration : durations )
        {
            List<Event<SingleValuedPair>> events = Slicer.filterByDuration( ts, a -> a.equals( nextDuration ) );

            for ( Event<SingleValuedPair> nextPair : events )
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
     * Constructs and iterates a time-series whose elements contain identical reference times and valid times.
     */

    @Test
    public void testIterateNonForecasts()
    {
        SortedSet<Event<SingleValuedPair>> data = new TreeSet<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        data.add( Event.of( Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        data.add( Event.of( Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        data.add( Event.of( Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ), SingleValuedPair.of( 7, 7 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        data.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );

        SampleMetadata meta = SampleMetadata.of();

        //Add the time-series, with only one for baseline
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeries( TimeSeries.of( data ) )
                                                 .setMetadata( meta )
                                                 .build();

        // Iterate by time
        int i = 1;
        for ( Event<SingleValuedPair> next : ts.eventIterator() )
        {
            assertEquals( next.getValue(), SingleValuedPair.of( i, i ) );
            i++;
        }
        assertEquals( 10, i ); // All elements iterated

        // Iterate by basis time
        int j = 1;
        for ( TimeSeries<SingleValuedPair> tsn : ts.getTimeSeries() )
        {
            assertEquals( tsn.getEvents().first().getValue(), SingleValuedPair.of( j, j ) );
            j++;
        }
        assertEquals( 2, j ); // All elements iterated

        // Iterate by duration
        int k = 1;

        SortedSet<Duration> durations = ts.getDurations();

        for ( Duration nextDuration : durations )
        {
            List<Event<SingleValuedPair>> events = Slicer.filterByDuration( ts, a -> a.equals( nextDuration ) );

            for ( Event<SingleValuedPair> next : events )
            {
                assertTrue( Duration.ZERO.equals( next.getDuration() ) );

                assertEquals( next.getValue(), SingleValuedPair.of( k, k ) );
                k++;
            }
        }
        assertEquals( 10, k ); // All elements iterated
    }

    /**
     * Checks that the climatology is preserved when building new time-series from existing time-series.
     */

    @Test
    public void testClimatologyIsPreserved()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();

        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3 );

        b.addTimeSeries( TimeSeries.of( basisTime,
                                         first ) )
         .setMetadata( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfSingleValuedPairs ts = b.build();

        //Add the first time-series and then append a second and third
        TimeSeriesOfSingleValuedPairsBuilder c = new TimeSeriesOfSingleValuedPairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( climatology.equals( c.build().getClimatology() ) );
    }

}

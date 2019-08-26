package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
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
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link TimeSeriesOfEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesOfEnsemblePairsTest
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
     * Tests the {@link TimeSeriesOfEnsemblePairs#referenceTimeIterator()} method.
     */

    @Test
    public void testReferenceTimeIterator()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<EnsemblePair>> first = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> second = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> third = new TreeSet<>();
        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( SECOND_TIME ),
                             EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( THIRD_TIME ),
                             EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( FOURTH_TIME ),
                             EnsemblePair.of( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( secondBasisTime,
                              Instant.parse( SIXTH_TIME ),
                              EnsemblePair.of( 4, new double[] { 4 } ) ) );
        second.add( Event.of( secondBasisTime,
                              Instant.parse( SEVENTH_TIME ),
                              EnsemblePair.of( 5, new double[] { 5 } ) ) );
        second.add( Event.of( secondBasisTime,
                              Instant.parse( EIGHTH_TIME ),
                              EnsemblePair.of( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( thirdBasisTime,
                             Instant.parse( TENTH_TIME ),
                             EnsemblePair.of( 7, new double[] { 7 } ) ) );
        third.add( Event.of( thirdBasisTime,
                             Instant.parse( ELEVENTH_TIME ),
                             EnsemblePair.of( 8, new double[] { 8 } ) ) );
        third.add( Event.of( thirdBasisTime,
                             Instant.parse( TWELFTH_TIME ),
                             EnsemblePair.of( 9, new double[] { 9 } ) ) );
        final SampleMetadata meta = SampleMetadata.of();
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeries( TimeSeries.of( firstBasisTime,
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
        for ( TimeSeries<EnsemblePair> next : ts.getTimeSeries() )
        {
            for ( Event<EnsemblePair> nextPair : next.getEvents() )
            {
                assertTrue( "Unexpected pair in basis-time iteration of time-series.",
                            nextPair.getValue().equals( EnsemblePair.of( nextValue, new double[] { nextValue } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the {@link TimeSeriesOfEnsemblePairs#getBaselineData()} method.
     */

    @Test
    public void testGetBaselineData()
    {
        //Build a time-series with two basis times
        SortedSet<Event<EnsemblePair>> values = new TreeSet<>();
        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime,
                              Instant.parse( SECOND_TIME ),
                              EnsemblePair.of( 1, new double[] { 1 } ) ) );
        values.add( Event.of( basisTime,
                              Instant.parse( THIRD_TIME ),
                              EnsemblePair.of( 2, new double[] { 2 } ) ) );
        values.add( Event.of( basisTime,
                              Instant.parse( FOURTH_TIME ),
                              EnsemblePair.of( 3, new double[] { 3 } ) ) );
        SampleMetadata meta = SampleMetadata.of();
        b.addTimeSeries( TimeSeries.of( basisTime,
                                         values ) );
        b.setMetadata( meta );

        //Check dataset dimensions
        assertTrue( Objects.isNull( b.build().getBaselineData() ) );

        b.addTimeSeriesForBaseline( TimeSeries.of( basisTime,
                                                    values ) );
        b.setMetadataForBaseline( meta );

        TimeSeriesOfEnsemblePairs baseline = b.build().getBaselineData();

        //Check dataset dimensions
        assertTrue( baseline.getDurations().size() == 3 && baseline.getReferenceTimes().size() == 1 );

        //Check dataset
        //Iterate and test
        int nextValue = 1;

        SortedSet<Duration> durations = baseline.getDurations();

        for ( Duration duration : durations )
        {
            List<Event<EnsemblePair>> events = Slicer.filterByDuration( baseline, a -> a.equals( duration ) );
            for ( Event<EnsemblePair> nextPair : events )
            {
                assertTrue( nextPair.getValue().equals( EnsemblePair.of( nextValue, new double[] { nextValue } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the addition of several time-series with a common reference time.
     */

    @Test
    public void testAddMultipleTimeSeriesWithSameReferenceTime()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        SortedSet<Event<EnsemblePair>> first = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> second = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> third = new TreeSet<>();

        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( basisTime,
                             Instant.parse( SECOND_TIME ),
                             EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( basisTime,
                             Instant.parse( THIRD_TIME ),
                             EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( basisTime,
                             Instant.parse( FOURTH_TIME ),
                             EnsemblePair.of( 3, new double[] { 3 } ) ) );
        SampleMetadata meta = SampleMetadata.of();
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3 );
        b.addTimeSeries( TimeSeries.of( basisTime,
                                         first ) )
         .addTimeSeriesForBaseline( TimeSeries.of( basisTime,
                                                    first ) )
         .setMetadata( meta )
         .setMetadataForBaseline( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfEnsemblePairs ts = b.build();

        //Add the first time-series and then append a second and third
        TimeSeriesOfEnsemblePairsBuilder c = new TimeSeriesOfEnsemblePairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( climatology.equals( c.build().getClimatology() ) );

        second.add( Event.of( basisTime,
                              Instant.parse( SECOND_TIME ),
                              EnsemblePair.of( 4, new double[] { 4 } ) ) );
        second.add( Event.of( basisTime,
                              Instant.parse( THIRD_TIME ),
                              EnsemblePair.of( 5, new double[] { 5 } ) ) );
        second.add( Event.of( basisTime,
                              Instant.parse( FOURTH_TIME ),
                              EnsemblePair.of( 6, new double[] { 6 } ) ) );
        third.add( Event.of( basisTime,
                             Instant.parse( SECOND_TIME ),
                             EnsemblePair.of( 7, new double[] { 7 } ) ) );
        third.add( Event.of( basisTime,
                             Instant.parse( THIRD_TIME ),
                             EnsemblePair.of( 8, new double[] { 8 } ) ) );
        third.add( Event.of( basisTime,
                             Instant.parse( FOURTH_TIME ),
                             EnsemblePair.of( 9, new double[] { 9 } ) ) );
        c.addTimeSeries( TimeSeries.of( basisTime,
                                         second ) )
         .addTimeSeries( TimeSeries.of( basisTime,
                                         third ) )
         .addTimeSeriesForBaseline( TimeSeries.of( basisTime,
                                                    second ) )
         .addTimeSeriesForBaseline( TimeSeries.of( basisTime,
                                                    third ) );

        TimeSeriesOfEnsemblePairs tsAppend = c.build();

        //Check dataset dimensions
        assertTrue( tsAppend.getDurations().size() == 3 && StreamSupport.stream( tsAppend.getTimeSeries()
                                                                                         .spliterator(),
                                                                                 false )
                                                                        .count() == 3 );
        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<EnsemblePair> nextSeries : tsAppend.getTimeSeries() )
        {
            for ( Event<EnsemblePair> nextPair : nextSeries.getEvents() )
            {
                assertTrue( nextPair.getValue().equals( EnsemblePair.of( nextValue, new double[] { nextValue } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void testExceptions()
    {
        SortedSet<Event<EnsemblePair>> first = new TreeSet<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( SECOND_TIME ),
                             EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( THIRD_TIME ),
                             EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( FOURTH_TIME ),
                             EnsemblePair.of( 3, new double[] { 3 } ) ) );
        final SampleMetadata meta = SampleMetadata.of();

        //Check for exceptions on the iterators
        TimeSeriesOfEnsemblePairsBuilder d = new TimeSeriesOfEnsemblePairsBuilder();
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) d.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                             first ) )
                                             .setMetadata( meta )
                                             .build();

        //Iterate
        exception.expect( NoSuchElementException.class );
        Iterator<TimeSeries<EnsemblePair>> noneSuchBasis = ts.getTimeSeries().iterator();
        noneSuchBasis.forEachRemaining( Objects::isNull );
        noneSuchBasis.next();

        //Mutate 
        exception.expect( UnsupportedOperationException.class );

        Iterator<TimeSeries<EnsemblePair>> immutableBasis = ts.getTimeSeries().iterator();
        immutableBasis.next();
        immutableBasis.remove();

    }

    /**
     * Tests the {@link TimeSeriesOfEnsemblePairs#toString()} method.
     */

    @Test
    public void testToString()
    {
        SortedSet<Event<EnsemblePair>> values = new TreeSet<>();
        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        SampleMetadata meta = SampleMetadata.of();
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            values.add( Event.of( basisTime,
                                  Instant.parse( "1985-01-01T" + String.format( "%02d", i ) + ZERO_ZEE ),
                                  EnsemblePair.of( 1, new double[] { 1 } ) ) );
            joiner.add( "(" + basisTime
                        + ",1985-01-01T"
                        + String.format( "%02d", i )
                        + ZERO_ZEE
                        + ",key: "
                        + "1.0 value: [1.0])" );
        }
        b.addTimeSeries( TimeSeries.of( basisTime,
                                         values ) )
         .setMetadata( meta );

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( FIFTH_TIME );
        SortedSet<Event<EnsemblePair>> otherValues = new TreeSet<>();
        for ( int i = 0; i < 5; i++ )
        {
            otherValues.add( Event.of( nextBasisTime,
                                       Instant.parse( "1985-01-02T" + String.format( "%02d", i ) + ZERO_ZEE ),
                                       EnsemblePair.of( 1, new double[] { 1 } ) ) );
            joiner.add( "(" + nextBasisTime
                        + ",1985-01-02T"
                        + String.format( "%02d", i )
                        + ZERO_ZEE
                        + ",key: "
                        + "1.0 value: [1.0])" );
        }

        b.addTimeSeries( TimeSeries.of( nextBasisTime,
                                         otherValues ) );
        assertTrue( joiner.toString().equals( b.build().toString() ) );
    }

    /**
     * Constructs and iterates an irregular time-series.
     */

    @Test
    public void testIterateIrregularTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<EnsemblePair>> first = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> second = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> third = new TreeSet<>();
        SortedSet<Event<EnsemblePair>> fourth = new TreeSet<>();
        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( SECOND_TIME ),
                             EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( "1985-01-01T08:00:00Z" ),
                             EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( firstBasisTime,
                             Instant.parse( "1985-01-01T09:00:00Z" ),
                             EnsemblePair.of( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( secondBasisTime,
                              Instant.parse( SEVENTH_TIME ),
                              EnsemblePair.of( 4, new double[] { 4 } ) ) );
        second.add( Event.of( secondBasisTime,
                              Instant.parse( "1985-01-02T04:00:00Z" ),
                              EnsemblePair.of( 5, new double[] { 5 } ) ) );
        second.add( Event.of( secondBasisTime,
                              Instant.parse( "1985-01-02T06:00:00Z" ),
                              EnsemblePair.of( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( thirdBasisTime,
                             Instant.parse( TENTH_TIME ),
                             EnsemblePair.of( 7, new double[] { 7 } ) ) );
        third.add( Event.of( thirdBasisTime,
                             Instant.parse( "1985-01-03T08:00:00Z" ),
                             EnsemblePair.of( 8, new double[] { 8 } ) ) );
        third.add( Event.of( thirdBasisTime,
                             Instant.parse( "1985-01-03T09:00:00Z" ),
                             EnsemblePair.of( 9, new double[] { 9 } ) ) );
        Instant fourthBasisTime = Instant.parse( "1985-01-04T00:00:00Z" );
        fourth.add( Event.of( fourthBasisTime,
                              Instant.parse( "1985-01-04T02:00:00Z" ),
                              EnsemblePair.of( 10, new double[] { 10 } ) ) );
        fourth.add( Event.of( fourthBasisTime,
                              Instant.parse( "1985-01-04T04:00:00Z" ),
                              EnsemblePair.of( 11, new double[] { 11 } ) ) );
        fourth.add( Event.of( fourthBasisTime,
                              Instant.parse( "1985-01-04T06:00:00Z" ),
                              EnsemblePair.of( 12, new double[] { 12 } ) ) );
        SampleMetadata meta = SampleMetadata.of();
        //Add the time-series, with only one for baseline
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeries( TimeSeries.of( firstBasisTime,
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

        for ( Duration duration : durations )
        {
            List<Event<EnsemblePair>> events = Slicer.filterByDuration( ts, a -> a.equals( duration ) );
            for ( Event<EnsemblePair> nextPair : events )
            {
                assertTrue( nextPair.getValue()
                                    .equals( EnsemblePair.of( expectedOrder[nextIndex],
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
        SortedSet<Event<EnsemblePair>> first = new TreeSet<>();

        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant basisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( basisTime,
                             Instant.parse( SECOND_TIME ),
                             EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( basisTime,
                             Instant.parse( THIRD_TIME ),
                             EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( basisTime,
                             Instant.parse( FOURTH_TIME ),
                             EnsemblePair.of( 3, new double[] { 3 } ) ) );
        SampleMetadata meta = SampleMetadata.of();
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3 );
        b.addTimeSeries( TimeSeries.of( basisTime,
                                         first ) )
         .setMetadata( meta )
         .setClimatology( climatology );

        //Build the first ts
        TimeSeriesOfEnsemblePairs ts = b.build();

        //Add the first time-series and then append a second and third
        TimeSeriesOfEnsemblePairsBuilder c = new TimeSeriesOfEnsemblePairsBuilder();
        c.addTimeSeries( ts );

        //Check that climatology has been preserved
        assertTrue( climatology.equals( c.build().getClimatology() ) );

    }

}

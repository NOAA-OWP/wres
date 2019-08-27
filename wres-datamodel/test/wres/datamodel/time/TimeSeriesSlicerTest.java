package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;

/**
 * Tests the {@link TimeSeriesSlicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeSeriesSlicerTest
{

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";
    private static final String SECOND_TIME = "1985-01-01T01:00:00Z";
    private static final String THIRD_TIME = "1985-01-01T02:00:00Z";
    private static final String FOURTH_TIME = "1985-01-01T03:00:00Z";
    private static final String FIFTH_TIME = "1985-01-02T00:00:00Z";
    private static final String SIXTH_TIME = "1985-01-02T01:00:00Z";
    private static final String SEVENTH_TIME = "1985-01-02T02:00:00Z";
    private static final String EIGHTH_TIME = "1985-01-02T03:00:00Z";
    private static final String NINTH_TIME = "1985-01-03T00:00:00Z";
    private static final String TENTH_TIME = "1985-01-03T01:00:00Z";
    private static final String ELEVENTH_TIME = "1985-01-03T02:00:00Z";
    private static final String TWELFTH_TIME = "1985-01-03T03:00:00Z";

    /**
     * Tests {@link TimeSeriesSlicer#getReferenceTimes()}.
     */

    @Test
    public void testGetReferenceTimes()
    {
        //Build a time-series with two basis times
        SortedSet<Event<SingleValuedPair>> values = new TreeSet<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        List<TimeSeries<SingleValuedPair>> timeSeries = new ArrayList<>();

        timeSeries.add( TimeSeries.of( basisTime, values ) );

        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        SortedSet<Event<SingleValuedPair>> otherValues = new TreeSet<>();
        otherValues.add( Event.of( nextBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        timeSeries.add( TimeSeries.of( nextBasisTime, otherValues ) );

        //Check dataset count
        SortedSet<Instant> times = TimeSeriesSlicer.getReferenceTimes( timeSeries );
        assertEquals( 2, times.size() );

        //Check the basis times
        assertEquals( basisTime, times.first() );
        Iterator<Instant> it = times.iterator();
        it.next();
        assertTrue( it.next().equals( nextBasisTime ) );
    }

    /**
     * Tests {@link TimeSeriesSlicer#getDurations()}.
     */

    @Test
    public void testGetDurations()
    {
        //Build a time-series with one basis time
        List<TimeSeries<SingleValuedPair>> timeSeries = new ArrayList<>();

        SortedSet<Event<SingleValuedPair>> values = new TreeSet<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        values.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        values.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );

        timeSeries.add( TimeSeries.of( basisTime, values ) );

        //Check dataset count
        SortedSet<Duration> actual = TimeSeriesSlicer.getDurations( timeSeries );
        SortedSet<Duration> expected = new TreeSet<>();
        expected.add( Duration.ofHours( 1 ) );
        expected.add( Duration.ofHours( 2 ) );
        expected.add( Duration.ofHours( 3 ) );

        assertEquals( expected, actual );

    }

    /**
     * Tests the {@link TimeSeriesSlicer#filterByReferenceTime(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testFilterByReferenceTime()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> third = new TreeSet<>();

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

        //Add the time-series
        List<TimeSeries<SingleValuedPair>> ts = List.of( TimeSeries.of( firstBasisTime,
                                                                        first ),
                                                         TimeSeries.of( secondBasisTime,
                                                                        second ),
                                                         TimeSeries.of( thirdBasisTime,
                                                                        third ) );

        //Iterate and test
        List<TimeSeries<SingleValuedPair>> filtered =
                TimeSeriesSlicer.filterByReferenceTime( ts, a -> a.equals( secondBasisTime ) );

        SortedSet<Instant> referenceTimes = TimeSeriesSlicer.getReferenceTimes( filtered );

        assertTrue( referenceTimes.size() == 1 );
        assertTrue( referenceTimes.first().equals( secondBasisTime ) );
        assertTrue( filtered.get( 0 )
                            .getEvents()
                            .first()
                            .getValue()
                            .equals( SingleValuedPair.of( 4, 4 ) ) );

        //Check for empty output on none filter
        List<TimeSeries<SingleValuedPair>> pairs =
                TimeSeriesSlicer.filterByReferenceTime( ts, a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) );

        SortedSet<Instant> sliced = TimeSeriesSlicer.getReferenceTimes( pairs );

        assertTrue( sliced.isEmpty() );

        //Check exceptional cases
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filterByReferenceTime( (List<TimeSeries<SingleValuedPair>>) null, null ) );
        assertThrows( NullPointerException.class,
                      () -> TimeSeriesSlicer.filterByReferenceTime( ts, null ) );
    }

}

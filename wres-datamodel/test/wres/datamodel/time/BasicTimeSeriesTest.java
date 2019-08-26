package wres.datamodel.time;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.time.BasicTimeSeries.BasicTimeSeriesBuilder;

/**
 * Tests the {@link BasicTimeSeries}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BasicTimeSeriesTest
{

    /**
     * Expected exception.
     */

    private static final String WHILE_ATTEMPTING_TO_MODIFY_AN_IMMUTABLE_TIME_SERIES =
            "While attempting to modify an immutable time-series.";

    /**
     * Fifth time for testing.
     */

    private static final String FIFTH_TIME = "1985-01-05T00:00:00Z";

    /**
     * Fourth time for testing.
     */

    private static final String FOURTH_TIME = "1985-01-04T00:00:00Z";

    /**
     * Third time for testing.
     */

    private static final String THIRD_TIME = "1985-01-03T00:00:00Z";

    /**
     * Second time for testing.
     */

    private static final String SECOND_TIME = "1985-01-02T00:00:00Z";

    /**
     * First time for testing.
     */

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Default time-series for testing.
     */

    private BasicTimeSeries<Double> defaultTimeSeries;


    @Before
    public void setUpBeforeEachTest()
    {
        BasicTimeSeriesBuilder<Double> b = new BasicTimeSeriesBuilder<>();
        SortedSet<Event<Double>> first = new TreeSet<>();
        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), 1.0 ) );
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), 2.0 ) );

        SortedSet<Event<Double>> second = new TreeSet<>();
        Instant secondBasisTime = Instant.parse( THIRD_TIME );
        second.add( Event.of( secondBasisTime, Instant.parse( FOURTH_TIME ), 3.0 ) );
        second.add( Event.of( secondBasisTime, Instant.parse( FIFTH_TIME ), 4.0 ) );

        defaultTimeSeries = (BasicTimeSeries<Double>) b.addTimeSeries( TimeSeriesA.of( firstBasisTime,
                                                                                       first ) )
                                                       .addTimeSeries( TimeSeriesA.of( secondBasisTime,
                                                                                       second ) )
                                                       .build();
    }

    /**
     * Test {@link BasicTimeSeries#eventIterator()}.
     */

    @Test
    public void testTimeIterator()
    {
        // Actual events
        List<Event<Double>> actual = new ArrayList<>();
        defaultTimeSeries.eventIterator().forEach( actual::add );

        // Expected events
        List<Event<Double>> expected = new ArrayList<>();
        expected.add( Event.of( Instant.parse( FIRST_TIME ),
                                Instant.parse( SECOND_TIME ),
                                1.0 ) );
        expected.add( Event.of( Instant.parse( FIRST_TIME ),
                                Instant.parse( THIRD_TIME ),
                                2.0 ) );
        expected.add( Event.of( Instant.parse( THIRD_TIME ),
                                Instant.parse( FOURTH_TIME ),
                                3.0 ) );
        expected.add( Event.of( Instant.parse( THIRD_TIME ),
                                Instant.parse( FIFTH_TIME ),
                                4.0 ) );

        assertTrue( actual.equals( expected ) );
    }

    /**
     * Test {@link BasicTimeSeries#referenceTimeIterator()}.
     */

    @Test
    public void testReferenceTimeIterator()
    {
        // Actual durations
        List<Instant> actual = new ArrayList<>();
        defaultTimeSeries.referenceTimeIterator().forEach( next -> actual.add( next.getReferenceTime() ) );

        // Expected durations
        List<Instant> expected = new ArrayList<>();
        expected.add( Instant.parse( FIRST_TIME ) );
        expected.add( Instant.parse( THIRD_TIME ) );

        assertTrue( actual.equals( expected ) );
    }

    /**
     * Tests {@link BasicTimeSeries#toString()}.
     */

    @Test
    public void testToString()
    {
        StringJoiner expected = new StringJoiner( System.lineSeparator() );
        expected.add( "(1985-01-01T00:00:00Z,1985-01-02T00:00:00Z,1.0)" );
        expected.add( "(1985-01-01T00:00:00Z,1985-01-03T00:00:00Z,2.0)" );
        expected.add( "(1985-01-03T00:00:00Z,1985-01-04T00:00:00Z,3.0)" );
        expected.add( "(1985-01-03T00:00:00Z,1985-01-05T00:00:00Z,4.0)" );

        assertTrue( expected.toString().equals( defaultTimeSeries.toString() ) );
    }

    /**
     * Tests {@link BasicTimeSeries#hasMultipleTimeSeries()}.
     */

    @Test
    public void testHasMultipleTimeSeries()
    {
        //Build a time-series with one basis time
        SortedSet<Event<SingleValuedPair>> values = new TreeSet<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        b.addTimeSeries( TimeSeriesA.of( basisTime, values ) );

        //Check dataset count
        assertTrue( b.build().getReferenceTimes().size() == 1 );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        SortedSet<Event<SingleValuedPair>> otherValues = new TreeSet<>();
        otherValues.add( Event.of( nextBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        b.addTimeSeries( TimeSeriesA.of( nextBasisTime, otherValues ) );

        assertTrue( b.build().getReferenceTimes().size() == 2 );
    }

    /**
     * Tests {@link BasicTimeSeries#getReferenceTimes()}.
     */

    @Test
    public void testGetReferenceTimes()
    {
        //Build a time-series with two basis times
        SortedSet<Event<SingleValuedPair>> values = new TreeSet<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        b.addTimeSeries( TimeSeriesA.of( basisTime, ReferenceTimeType.UNKNOWN, values ) );

        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        SortedSet<Event<SingleValuedPair>> otherValues = new TreeSet<>();
        otherValues.add( Event.of( nextBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        b.addTimeSeries( TimeSeriesA.of( nextBasisTime, otherValues ) );

        TimeSeriesCollection<SingleValuedPair> pairs = b.build();

        //Check dataset count
        assertTrue( pairs.getReferenceTimes().size() == 2 );

        //Check the basis times
        assertTrue( pairs.getReferenceTimes().first().equals( basisTime ) );
        Iterator<Instant> it = pairs.getReferenceTimes().iterator();
        it.next();
        assertTrue( it.next().equals( nextBasisTime ) );
    }

    /**
     * Tests the {@link BasicTimeSeries#getDurations()} method.
     */

    @Test
    public void testGetDurations()
    {
        //Build a time-series with two basis times
        SortedSet<Event<SingleValuedPair>> values = new TreeSet<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        values.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        values.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );

        b.addTimeSeries( TimeSeriesA.of( basisTime, values ) );

        //Check dataset count
        assertTrue( b.build().getDurations().size() == 3 );
        //Check the lead times
        assertTrue( b.build().getDurations().contains( Duration.ofDays( 1 ) ) );
        assertTrue( b.build().getDurations().contains( Duration.ofDays( 2 ) ) );
        assertTrue( b.build().getDurations().contains( Duration.ofDays( 3 ) ) );
    }

    /**
     * Confirms that the {@link BasicTimeSeries#eventIterator()} throws an iteration exception when expected.
     */

    @Test
    public void testTimeIteratorThrowsNoSuchElementException()
    {
        exception.expect( NoSuchElementException.class );
        exception.expectMessage( "No more events to iterate." );

        Iterator<Event<Double>> noneSuchElement = defaultTimeSeries.eventIterator().iterator();
        noneSuchElement.forEachRemaining( Objects::isNull );
        noneSuchElement.next();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#referenceTimeIterator()} throws an iteration exception when expected.
     */

    @Test
    public void testReferenceTimeIteratorThrowsNoSuchElementException()
    {
        exception.expect( NoSuchElementException.class );
        exception.expectMessage( "No more reference times to iterate." );

        Iterator<TimeSeriesA<Double>> noneSuchBasis = defaultTimeSeries.referenceTimeIterator().iterator();
        noneSuchBasis.forEachRemaining( Objects::isNull );
        noneSuchBasis.next();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#eventIterator()} throws an exception when attempting to mutate the 
     * time-series.
     */

    @Test
    public void testTimeIteratorThrowsExceptionOnAttemptToMutate()
    {
        exception.expect( UnsupportedOperationException.class );
        exception.expectMessage( WHILE_ATTEMPTING_TO_MODIFY_AN_IMMUTABLE_TIME_SERIES );

        Iterator<Event<Double>> immutableTimeSeries = defaultTimeSeries.eventIterator().iterator();
        immutableTimeSeries.next();
        immutableTimeSeries.remove();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#basisTimeIterator} throws an exception when attempting to mutate the 
     * time-series.
     */

    @Test
    public void testReferemceTimeIteratorThrowsExceptionOnAttemptToMutate()
    {
        exception.expect( UnsupportedOperationException.class );
        exception.expectMessage( WHILE_ATTEMPTING_TO_MODIFY_AN_IMMUTABLE_TIME_SERIES );

        Iterator<TimeSeriesA<Double>> immutableTimeSeries = defaultTimeSeries.referenceTimeIterator().iterator();
        immutableTimeSeries.next();
        immutableTimeSeries.remove();
    }

    /**
     * Confirms an expected exception when constructing a {@link BasicTimeSeries} with a null time-series.
     */

    @Test
    public void testForExceptionOnNullTimeSeries()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "One or more time-series is null." );
        new BasicTimeSeriesBuilder<Double>().addTimeSeries( (TimeSeriesA<Double>) null ).build();

    }

}

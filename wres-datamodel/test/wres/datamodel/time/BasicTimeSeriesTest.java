package wres.datamodel.time;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;

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
        List<Event<Double>> first = new ArrayList<>();
        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), 1.0 ) );
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), 2.0 ) );
        List<Event<Double>> second = new ArrayList<>();
        Instant secondBasisTime = Instant.parse( THIRD_TIME );
        second.add( Event.of( secondBasisTime, Instant.parse( FOURTH_TIME ), 3.0 ) );
        second.add( Event.of( secondBasisTime, Instant.parse( FIFTH_TIME ), 4.0 ) );

        defaultTimeSeries = (BasicTimeSeries<Double>) b.addTimeSeries( first )
                                                       .addTimeSeries( second )
                                                       .build();
    }

    /**
     * Test {@link BasicTimeSeries#timeIterator()}.
     */

    @Test
    public void testTimeIterator()
    {
        // Actual events
        List<Event<Double>> actual = new ArrayList<>();
        defaultTimeSeries.timeIterator().forEach( actual::add );

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
     * Test {@link BasicTimeSeries#durationIterator()}.
     */

    @Test
    public void testDurationIterator()
    {
        // Actual durations
        List<Duration> actual = new ArrayList<>();
        defaultTimeSeries.durationIterator().forEach( next -> actual.addAll( next.getDurations() ) );

        // Expected durations
        List<Duration> expected = new ArrayList<>();
        expected.add( Duration.ofDays( 1 ) );
        expected.add( Duration.ofDays( 2 ) );

        assertTrue( actual.equals( expected ) );
    }

    /**
     * Test {@link BasicTimeSeries#basisTimeIterator()}.
     */

    @Test
    public void testBasisTimeIterator()
    {
        // Actual durations
        List<Instant> actual = new ArrayList<>();
        defaultTimeSeries.basisTimeIterator().forEach( next -> actual.add( next.getEarliestBasisTime() ) );

        // Expected durations
        List<Instant> expected = new ArrayList<>();
        expected.add( Instant.parse( FIRST_TIME ) );
        expected.add( Instant.parse( THIRD_TIME ) );

        assertTrue( actual.equals( expected ) );
    }

    /**
     * Test {@link BasicTimeSeries#getEarliestBasisTime()}.
     */

    @Test
    public void testGetEarliestBasisTime()
    {
        assertTrue( Instant.parse( FIRST_TIME ).equals( defaultTimeSeries.getEarliestBasisTime() ) );
    }

    /**
     * Test {@link BasicTimeSeries#getLatestBasisTime()}.
     */

    @Test
    public void testGetLatestBasisTime()
    {
        assertTrue( Instant.parse( THIRD_TIME ).equals( defaultTimeSeries.getLatestBasisTime() ) );
    }

    /**
     * Tests {@link BasicTimeSeries#isRegular()}.
     */

    @Test
    public void testIsRegular()
    {
        assertTrue( "Expected a regular time-series.", defaultTimeSeries.isRegular() );
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
     * Tests {@link BasicTimeSeries#getRegularDuration()}.
     */

    @Test
    public void testGetRegularDuration()
    {
        //Build a time-series with one basis time
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( firstBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        TimeSeries<SingleValuedPair> ts = b.addTimeSeries( first ).build();
        Duration benchmark = Duration.ofDays( 1 );

        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    ts.getRegularDuration().equals( benchmark ) );

        //Add more data and test again
        first.add( Event.of( firstBasisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );
        first.add( Event.of( firstBasisTime, Instant.parse( FIFTH_TIME ), SingleValuedPair.of( 4, 4 ) ) );

        BasicTimeSeriesBuilder<SingleValuedPair> c = new BasicTimeSeriesBuilder<>();
        TimeSeries<SingleValuedPair> tsSecond = c.addTimeSeries( first ).build();

        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    tsSecond.getRegularDuration().equals( benchmark ) );

        //Add an irregular timestep and check for null output
        first.add( Event.of( firstBasisTime, Instant.parse( "1985-01-07T00:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        BasicTimeSeriesBuilder<SingleValuedPair> d = new BasicTimeSeriesBuilder<>();
        TimeSeries<SingleValuedPair> tsThird = d.addTimeSeries( first ).build();

        assertTrue( "Expected an irregular time-series.",
                    Objects.isNull( tsThird.getRegularDuration() ) );
    }

    /**
     * Tests {@link BasicTimeSeries#hasMultipleTimeSeries()}.
     */

    @Test
    public void testHasMultipleTimeSeries()
    {
        //Build a time-series with one basis time
        List<Event<SingleValuedPair>> values = new ArrayList<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        b.addTimeSeries( values );

        //Check dataset count
        assertFalse( "Expected a time-series with one basis time.", b.build().hasMultipleTimeSeries() );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        b.addTimeSeries( Arrays.asList( Event.of( nextBasisTime,
                                                      Instant.parse( SECOND_TIME ),
                                                      SingleValuedPair.of( 1, 1 ) ) ) );

        assertTrue( "Expected a time-series with multiple basis times.", b.build().hasMultipleTimeSeries() );
    }

    /**
     * Tests {@link BasicTimeSeries#getBasisTimes()}.
     */

    @Test
    public void testGetBasisTimes()
    {
        //Build a time-series with two basis times
        List<Event<SingleValuedPair>> values = new ArrayList<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        b.addTimeSeries( values );

        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        b.addTimeSeries( Arrays.asList( Event.of( nextBasisTime,
                                                      Instant.parse( SECOND_TIME ),
                                                      SingleValuedPair.of( 1, 1 ) ) ) );
        TimeSeries<SingleValuedPair> pairs = b.build();

        //Check dataset count
        assertTrue( "Expected a time-series with two basis times.", pairs.getBasisTimes().size() == 2 );

        //Check the basis times
        assertTrue( "First basis time missing from time-series.",
                    pairs.getBasisTimes().first().equals( basisTime ) );
        Iterator<Instant> it = pairs.getBasisTimes().iterator();
        it.next();
        assertTrue( "Second basis time missing from time-series.", it.next().equals( nextBasisTime ) );
    }

    /**
     * Tests the {@link BasicTimeSeries#getDurations()} method.
     */

    @Test
    public void testGetDurations()
    {
        //Build a time-series with two basis times
        List<Event<SingleValuedPair>> values = new ArrayList<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        values.add( Event.of( basisTime, Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 2 ) ) );
        values.add( Event.of( basisTime, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 3 ) ) );

        b.addTimeSeries( values );
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
     * Confirms that the {@link BasicTimeSeries#timeIterator()} throws an iteration exception when expected.
     */

    @Test
    public void testTimeIteratorThrowsNoSuchElementException()
    {
        exception.expect( NoSuchElementException.class );
        exception.expectMessage( "No more events to iterate." );

        Iterator<Event<Double>> noneSuchElement = defaultTimeSeries.timeIterator().iterator();
        noneSuchElement.forEachRemaining( Objects::isNull );
        noneSuchElement.next();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#basisTimeIterator()} throws an iteration exception when expected.
     */

    @Test
    public void testBasisTimeIteratorThrowsNoSuchElementException()
    {
        exception.expect( NoSuchElementException.class );
        exception.expectMessage( "No more basis times to iterate." );

        Iterator<TimeSeries<Double>> noneSuchBasis = defaultTimeSeries.basisTimeIterator().iterator();
        noneSuchBasis.forEachRemaining( Objects::isNull );
        noneSuchBasis.next();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#durationIterator()} throws an iteration exception when expected.
     */

    @Test
    public void testDurationIteratorThrowsNoSuchElementException()
    {
        exception.expect( NoSuchElementException.class );
        exception.expectMessage( "No more durations to iterate." );

        Iterator<TimeSeries<Double>> noneSuchDuration = defaultTimeSeries.durationIterator().iterator();
        noneSuchDuration.forEachRemaining( Objects::isNull );
        noneSuchDuration.next();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#timeIterator()} throws an exception when attempting to mutate the 
     * time-series.
     */

    @Test
    public void testTimeIteratorThrowsExceptionOnAttemptToMutate()
    {
        exception.expect( UnsupportedOperationException.class );
        exception.expectMessage( WHILE_ATTEMPTING_TO_MODIFY_AN_IMMUTABLE_TIME_SERIES );

        Iterator<Event<Double>> immutableTimeSeries = defaultTimeSeries.timeIterator().iterator();
        immutableTimeSeries.next();
        immutableTimeSeries.remove();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#durationIterator()} throws an exception when attempting to mutate the 
     * time-series.
     */

    @Test
    public void testDurationIteratorThrowsExceptionOnAttemptToMutate()
    {
        exception.expect( UnsupportedOperationException.class );
        exception.expectMessage( WHILE_ATTEMPTING_TO_MODIFY_AN_IMMUTABLE_TIME_SERIES );

        Iterator<TimeSeries<Double>> immutableTimeSeries = defaultTimeSeries.durationIterator().iterator();
        immutableTimeSeries.next();
        immutableTimeSeries.remove();
    }

    /**
     * Confirms that the {@link BasicTimeSeries#basisTimeIterator} throws an exception when attempting to mutate the 
     * time-series.
     */

    @Test
    public void testBasisTimeIteratorThrowsExceptionOnAttemptToMutate()
    {
        exception.expect( UnsupportedOperationException.class );
        exception.expectMessage( WHILE_ATTEMPTING_TO_MODIFY_AN_IMMUTABLE_TIME_SERIES );

        Iterator<TimeSeries<Double>> immutableTimeSeries = defaultTimeSeries.basisTimeIterator().iterator();
        immutableTimeSeries.next();
        immutableTimeSeries.remove();
    }

    /**
     * Confirms an expected exception when constructing a {@link BasicTimeSeries} with a null event.
     */

    @Test
    public void testForExceptionOnNullEvent()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "One or more time-series has null values." );

        List<Event<Double>> withNulls = new ArrayList<>();
        withNulls.add( Event.of( Instant.parse( FIRST_TIME ),
                                 Instant.parse( "1985-01-01T01:00:00Z" ),
                                 1.0 ) );
        withNulls.add( null );
        new BasicTimeSeriesBuilder<Double>().addTimeSeries( withNulls ).build();
    }

    /**
     * Confirms an expected exception when constructing a {@link BasicTimeSeries} with a null time-series.
     */

    @Test
    public void testForExceptionOnNullTimeSeries()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "One or more time-series is null." );
        new BasicTimeSeriesBuilder<Double>().addTimeSeries( (List<Event<Double>>)null ).build();

    }

}

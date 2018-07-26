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

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.time.Event;
import wres.datamodel.time.BasicTimeSeries;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.BasicTimeSeries.BasicTimeSeriesBuilder;

/**
 * Tests the {@link BasicTimeSeries}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BasicTimeSeriesTest
{

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
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), 1.0 ) );
        first.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), 2.0 ) );
        List<Event<Double>> second = new ArrayList<>();
        Instant secondBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), 3.0 ) );
        second.add( Event.of( Instant.parse( "1985-01-05T00:00:00Z" ), 4.0 ) );

        defaultTimeSeries = (BasicTimeSeries<Double>) b.addTimeSeriesData( firstBasisTime, first )
                                                       .addTimeSeriesData( secondBasisTime, second )
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
        expected.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), 1.0 ) );
        expected.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), 2.0 ) );
        expected.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), 3.0 ) );
        expected.add( Event.of( Instant.parse( "1985-01-05T00:00:00Z" ), 4.0 ) );

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
        expected.add( Instant.parse( "1985-01-01T00:00:00Z" ) );
        expected.add( Instant.parse( "1985-01-03T00:00:00Z" ) );

        assertTrue( actual.equals( expected ) );
    }

    /**
     * Test {@link BasicTimeSeries#getEarliestBasisTime()}.
     */

    @Test
    public void testGetEarliestBasisTime()
    {
        assertTrue( Instant.parse( "1985-01-01T00:00:00Z" ).equals( defaultTimeSeries.getEarliestBasisTime() ) );
    }

    /**
     * Test {@link BasicTimeSeries#getLatestBasisTime()}.
     */

    @Test
    public void testGetLatestBasisTime()
    {
        assertTrue( Instant.parse( "1985-01-03T00:00:00Z" ).equals( defaultTimeSeries.getLatestBasisTime() ) );
    }

    /**
     * Test {@link BasicTimeSeries#getRawData()}.
     */

    @Test
    public void testGetRawData()
    {
        List<Event<Double>> first = new ArrayList<>();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), 1.0 ) );
        first.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), 2.0 ) );
        List<Event<Double>> second = new ArrayList<>();
        Instant secondBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), 3.0 ) );
        second.add( Event.of( Instant.parse( "1985-01-05T00:00:00Z" ), 4.0 ) );

        List<Event<List<Event<Double>>>> expected = new ArrayList<>();
        expected.add( Event.of( firstBasisTime, first ) );
        expected.add( Event.of( secondBasisTime, second ) );

        assertTrue( expected.equals( defaultTimeSeries.getRawData() ) );
    }

    /**
     * Confirms that the {@link BasicTimeSeries#getRawData()} returns an immutable outer container.
     */

    @Test
    public void testGetRawDataIsImmutableOuter()
    {
        List<Event<List<Event<Double>>>> rawData = defaultTimeSeries.getRawData();

        exception.expect( UnsupportedOperationException.class );
        Instant time = Instant.parse( "1985-01-03T00:00:00Z" );
        rawData.add( Event.of( time, Arrays.asList( Event.of( time, 1.0 ) ) ) );

    }

    /**
     * Confirms that the {@link BasicTimeSeries#getRawData()} returns immutable inner containers.
     */

    @Test
    public void testGetRawDataIsImmutableInner()
    {
        List<Event<List<Event<Double>>>> rawData = defaultTimeSeries.getRawData();

        exception.expect( UnsupportedOperationException.class );

        rawData.get( 0 ).getValue().add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), 1.0 ) );
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
        expected.add( "(1985-01-02T00:00:00Z,1.0)" );
        expected.add( "(1985-01-03T00:00:00Z,2.0)" );
        expected.add( "(1985-01-04T00:00:00Z,3.0)" );
        expected.add( "(1985-01-05T00:00:00Z,4.0)" );

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
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );

        TimeSeries<SingleValuedPair> ts = b.addTimeSeriesData( firstBasisTime, first )
                                        .build();
        Duration benchmark = Duration.ofDays( 1 );
        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    ts.getRegularDuration().equals( benchmark ) );

        //Add more data and test again
        first.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), DataFactory.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), DataFactory.pairOf( 3, 3 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-05T00:00:00Z" ), DataFactory.pairOf( 4, 4 ) ) );

        BasicTimeSeriesBuilder<SingleValuedPair> c = new BasicTimeSeriesBuilder<>();
        TimeSeries<SingleValuedPair> tsSecond = c.addTimeSeriesData( firstBasisTime, first )
                                              .build();
        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    tsSecond.getRegularDuration().equals( benchmark ) );

        //Add an irregular timestep and check for null output
        first.add( Event.of( Instant.parse( "1985-01-07T00:00:00Z" ), DataFactory.pairOf( 4, 4 ) ) );
        BasicTimeSeriesBuilder<SingleValuedPair> d = new BasicTimeSeriesBuilder<>();
        TimeSeries<SingleValuedPair> tsThird = d.addTimeSeriesData( firstBasisTime, first )
                                             .build();
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
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );

        b.addTimeSeriesData( basisTime, values );

        //Check dataset count
        assertFalse( "Expected a time-series with one basis time.", b.build().hasMultipleTimeSeries() );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
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
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        b.addTimeSeriesData( basisTime, values );

        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        TimeSeries<SingleValuedPair> pairs = b.build();

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
     * Tests the {@link BasicTimeSeries#getDurations()} method.
     */

    @Test
    public void testGetDurations()
    {
        //Build a time-series with two basis times
        List<Event<SingleValuedPair>> values = new ArrayList<>();
        BasicTimeSeriesBuilder<SingleValuedPair> b = new BasicTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), DataFactory.pairOf( 2, 2 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), DataFactory.pairOf( 3, 3 ) ) );

        b.addTimeSeriesData( basisTime, values );
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
        noneSuchElement.forEachRemaining( a -> a.equals( null ) );
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
        noneSuchBasis.forEachRemaining( a -> a.equals( null ) );
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
        noneSuchDuration.forEachRemaining( a -> a.equals( null ) );
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
        exception.expectMessage( "While attempting to modify an immutable time-series." );

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
        exception.expectMessage( "While attempting to modify an immutable time-series." );

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
        exception.expectMessage( "While attempting to modify an immutable time-series." );

        Iterator<TimeSeries<Double>> immutableTimeSeries = defaultTimeSeries.basisTimeIterator().iterator();
        immutableTimeSeries.next();
        immutableTimeSeries.remove();
    }

    /**
     * Confirms an expected exception when constructing a {@link BasicTimeSeries} with null input.
     */

    @Test
    public void testForExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Cannot build a time-series with one or more null events." );

        List<Event<Double>> withNulls = new ArrayList<>();
        withNulls.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), 1.0 ) );
        withNulls.add( null );
        new BasicTimeSeriesBuilder<Double>().addTimeSeriesData( Instant.parse( "1985-01-01T00:00:00Z" ), withNulls )
                                            .build();

        exception.expect( MetricInputException.class );
        exception.expectMessage( "Cannot build a time-series with one or more null time-series." );
        new BasicTimeSeriesBuilder<Double>().addTimeSeriesData( Instant.parse( "1985-01-01T00:00:00Z" ), null )
                                            .build();

    }

}

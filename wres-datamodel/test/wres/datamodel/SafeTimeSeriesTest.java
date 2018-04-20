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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.SafeTimeSeries.SafeTimeSeriesBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link SafeTimeSeries}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SafeTimeSeriesTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Data factory.
     */

    private DataFactory metIn;

    /**
     * Metadata factory.
     */

    private MetadataFactory metaFac;

    /**
     * Default time-series for testing.
     */

    private TimeSeries<Double> defaultTimeSeries;


    @Before
    public void setUpBeforeEachTest()
    {
        metIn = DefaultDataFactory.getInstance();
        metaFac = metIn.getMetadataFactory();
        SafeTimeSeriesBuilder<Double> b = new SafeTimeSeriesBuilder<>();
        List<Event<Double>> first = new ArrayList<>();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), 1.0 ) );
        first.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), 2.0 ) );
        List<Event<Double>> second = new ArrayList<>();
        Instant secondBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), 3.0 ) );
        second.add( Event.of( Instant.parse( "1985-01-05T00:00:00Z" ), 4.0 ) );

        defaultTimeSeries = b.addTimeSeriesData( firstBasisTime, first )
                             .addTimeSeriesData( secondBasisTime, second )
                             .build();
    }

    /**
     * Test {@link SafeTimeSeries#timeIterator()}.
     */

    @Test
    public void testTimeIterator()
    {
        // Not null
        assertTrue( Objects.nonNull( defaultTimeSeries.timeIterator() ) );

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
     * Tests {@link SafeTimeSeries#isRegular()}.
     */

    @Test
    public void testIsRegular()
    {
        assertTrue( "Expected a regular time-series.", defaultTimeSeries.isRegular() );
    }

    /**
     * Tests {@link SafeTimeSeries#getRegularDuration()}.
     */

    @Test
    public void testGetRegularDuration()
    {
        //Build a time-series with one basis time
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        SafeTimeSeriesBuilder<PairOfDoubles> b = new SafeTimeSeriesBuilder<>();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );

        TimeSeries<PairOfDoubles> ts = b.addTimeSeriesData( firstBasisTime, first )
                                        .build();
        Duration benchmark = Duration.ofDays( 1 );
        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    ts.getRegularDuration().equals( benchmark ) );

        //Add more data and test again
        first.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-05T00:00:00Z" ), metIn.pairOf( 4, 4 ) ) );

        SafeTimeSeriesBuilder<PairOfDoubles> c = new SafeTimeSeriesBuilder<>();
        TimeSeries<PairOfDoubles> tsSecond = c.addTimeSeriesData( firstBasisTime, first )
                                              .build();
        assertTrue( "Expected a regular time-series with a duration of '" + benchmark
                    + "'.",
                    tsSecond.getRegularDuration().equals( benchmark ) );

        //Add an irregular timestep and check for null output
        first.add( Event.of( Instant.parse( "1985-01-07T00:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        SafeTimeSeriesBuilder<PairOfDoubles> d = new SafeTimeSeriesBuilder<>();
        TimeSeries<PairOfDoubles> tsThird = d.addTimeSeriesData( firstBasisTime, first )
                                             .build();
        assertTrue( "Expected an irregular time-series.",
                    Objects.isNull( tsThird.getRegularDuration() ) );
    }

    /**
     * Tests {@link SafeTimeSeries#hasMultipleTimeSeries()}.
     */

    @Test
    public void testHasMultipleTimeSeries()
    {
        //Build a time-series with one basis time
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesBuilder<PairOfDoubles> b = new SafeTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );

        b.addTimeSeriesData( basisTime, values );

        //Check dataset count
        assertFalse( "Expected a time-series with one basis time.", b.build().hasMultipleTimeSeries() );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        assertTrue( "Expected a time-series with multiple basis times.", b.build().hasMultipleTimeSeries() );
    }

    /**
     * Tests {@link SafeTimeSeries#getBasisTimes()}.
     */

    @Test
    public void testGetBasisTimes()
    {
        //Build a time-series with two basis times
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesBuilder<PairOfDoubles> b = new SafeTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        b.addTimeSeriesData( basisTime, values );

        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        TimeSeries<PairOfDoubles> pairs = b.build();

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
     * Tests the {@link SafeTimeSeries#getDurations()} method.
     */

    @Test
    public void testGetDurations()
    {
        //Build a time-series with two basis times
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesBuilder<PairOfDoubles> b = new SafeTimeSeriesBuilder<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), metIn.pairOf( 3, 3 ) ) );

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
     * Confirms that the {@link SafeTimeSeries#timeIterator()} throws an iteration exception when expected.
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
     * Confirms that the {@link SafeTimeSeries#basisTimeIterator()} throws an iteration exception when expected.
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
     * Confirms that the {@link SafeTimeSeries#durationIterator()} throws an iteration exception when expected.
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
     * Confirms that the {@link SafeTimeSeries#timeIterator()} throws an exception when attempting to mutate the 
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
     * Confirms that the {@link SafeTimeSeries#durationIterator()} throws an exception when attempting to mutate the 
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
     * Confirms that the {@link SafeTimeSeries#basisTimeIterator} throws an exception when attempting to mutate the 
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
     * Confirms an expected exception when constructing a {@link SafeTimeSeries} with null input.
     */

    @Test
    public void testForExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Cannot build a time-series with one or more null events." );

        List<Event<Double>> withNulls = new ArrayList<>();
        withNulls.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), 1.0 ) );
        withNulls.add( null );
        new SafeTimeSeriesBuilder<Double>().addTimeSeriesData( Instant.parse( "1985-01-01T00:00:00Z" ), withNulls )
                                           .build();

        exception.expect( MetricInputException.class );
        exception.expectMessage( "Cannot build a time-series with one or more null time-series." );
        new SafeTimeSeriesBuilder<Double>().addTimeSeriesData( Instant.parse( "1985-01-01T00:00:00Z" ), null )
                                           .build();

    }

}

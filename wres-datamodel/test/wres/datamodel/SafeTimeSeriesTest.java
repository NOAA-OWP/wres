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
import wres.datamodel.SafeTimeSeriesOfSingleValuedPairs.SafeTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Metadata;
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
     * Test the {@link SafeTimeSeries#timeIterator()}.
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
     * Confirm that the {@link SafeTimeSeries#timeIterator()} throws an iteration exception when expected.
     */

    @Test
    public void testTimeIteratorThrowsNoSuchElementException()
    {
        exception.expect( NoSuchElementException.class );
        exception.expectMessage( "No more events to iterate." );
        
        Iterator<Event<Double>> iterator = defaultTimeSeries.timeIterator().iterator();
        iterator.next();
        iterator.next();
        iterator.next();
        iterator.next();
        iterator.next();
    }


    /**
     * Tests the {@link SafeTimeSeries#isRegular()} method.
     */

    @Test
    public void testIsRegular()
    {
        assertTrue( "Expected a regular time-series.", defaultTimeSeries.isRegular() );
    }

    /**
     * Tests the {@link SafeTimeSeries#getRegularDuration()} method.
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
     * Tests the {@link SafeTimeSeries#hasMultipleTimeSeries()} method.
     */

    @Test
    public void testHasMultipleTimeSeries()
    {
        //Build a time-series with one basis time
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );
        b.addTimeSeriesDataForBaseline( basisTime, values ).setMetadataForBaseline( meta );

        //Check dataset count
        assertFalse( "Expected a time-series with one basis time.", b.build().hasMultipleTimeSeries() );
        assertFalse( "Expected a time-series with one basis time.",
                     b.build().getBaselineData().hasMultipleTimeSeries() );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        b.addTimeSeriesDataForBaseline( nextBasisTime, values ).setMetadataForBaseline( meta );
        assertTrue( "Expected a time-series with multiple basis times.", b.build().hasMultipleTimeSeries() );
        assertTrue( "Expected a time-series with multiple basis times.",
                    b.build().getBaselineData().hasMultipleTimeSeries() );
    }

    /**
     * Tests the {@link SafeTimeSeries#getBasisTimes()}.
     */

    @Test
    public void testGetBasisTimes()
    {
        //Build a time-series with two basis times
        List<Event<PairOfDoubles>> values = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values )
         .setMetadata( meta );
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addTimeSeriesData( nextBasisTime, values );
        SafeTimeSeriesOfSingleValuedPairs pairs = b.build();
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
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        values.add( Event.of( Instant.parse( "1985-01-04T00:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Metadata meta = metaFac.getMetadata();
        b.addTimeSeriesData( basisTime, values ).setMetadata( meta );
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
    public void testExceptions()
    {
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        final Metadata meta = metaFac.getMetadata();

        //Check for exceptions on the iterators
        SafeTimeSeriesOfSingleValuedPairsBuilder d = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) d.addTimeSeriesData( firstBasisTime, first )
                                                 .setMetadata( meta )
                                                 .build();


        // Iterate
        exception.expect( NoSuchElementException.class );
        Iterator<TimeSeries<PairOfDoubles>> noneSuchBasis = ts.basisTimeIterator().iterator();
        noneSuchBasis.forEachRemaining( a -> a.equals( null ) );
        noneSuchBasis.next();

        Iterator<TimeSeries<PairOfDoubles>> noneSuchDuration = ts.durationIterator().iterator();
        noneSuchDuration.forEachRemaining( a -> a.equals( null ) );
        noneSuchDuration.next();

        Iterator<Event<PairOfDoubles>> noneSuchElement = ts.timeIterator().iterator();
        noneSuchElement.forEachRemaining( a -> a.equals( null ) );
        noneSuchElement.next();

        //Mutate 
        exception.expect( UnsupportedOperationException.class );

        Iterator<TimeSeries<PairOfDoubles>> immutableBasis = ts.basisTimeIterator().iterator();
        immutableBasis.next();
        immutableBasis.remove();

        Iterator<TimeSeries<PairOfDoubles>> immutableDuration = ts.durationIterator().iterator();
        immutableDuration.next();
        immutableDuration.remove();

        //Construct with null input
        exception.expect( MetricInputException.class );

        List<Event<PairOfDoubles>> withNulls = new ArrayList<>();
        withNulls.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        withNulls.add( null );
        new SafeTimeSeriesOfSingleValuedPairsBuilder().addTimeSeriesData( firstBasisTime, withNulls )
                                                      .setMetadata( meta )
                                                      .build();

        //Construct with null input for baseline
        List<Event<PairOfDoubles>> withNullsForBaseline = new ArrayList<>();
        withNullsForBaseline.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        withNullsForBaseline.add( null );
        new SafeTimeSeriesOfSingleValuedPairsBuilder().addTimeSeriesData( firstBasisTime, first )
                                                      .addTimeSeriesDataForBaseline( firstBasisTime,
                                                                                     withNullsForBaseline )
                                                      .setMetadata( meta )
                                                      .setMetadataForBaseline( meta )
                                                      .build();
    }

}

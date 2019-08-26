package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.Slicer;
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

        defaultTimeSeries = (BasicTimeSeries<Double>) b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                       first ) )
                                                       .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                       second ) )
                                                       .build();
    }

    /**
     * Test {@link BasicTimeSeries#referenceTimeIterator()}.
     */

    @Test
    public void testReferenceTimeIterator()
    {
        // Actual durations
        List<Instant> actual = new ArrayList<>();
        defaultTimeSeries.get().forEach( next -> actual.add( next.getReferenceTime() ) );

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

        Instant basisTime = Instant.parse( FIRST_TIME );
        values.add( Event.of( basisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        List<TimeSeries<SingleValuedPair>> timeSeries = new ArrayList<>();
        
        timeSeries.add( TimeSeries.of( basisTime, values ) );

        //Check dataset count
        SortedSet<Instant> referenceTimes = Slicer.getReferenceTimes( timeSeries );
        
        assertEquals( 1, referenceTimes.size() );

        //Add another time-series
        Instant nextBasisTime = Instant.parse( SECOND_TIME );
        SortedSet<Event<SingleValuedPair>> otherValues = new TreeSet<>();
        otherValues.add( Event.of( nextBasisTime, Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 1 ) ) );

        timeSeries.add( TimeSeries.of( nextBasisTime, otherValues ) );

        SortedSet<Instant> referenceTimesTwo = Slicer.getReferenceTimes( timeSeries );
        
        assertEquals( 2, referenceTimesTwo.size() );
    }

    /**
     * Confirms that the {@link BasicTimeSeries#basisTimeIterator} throws an exception when attempting to mutate the 
     * time-series.
     */

    @Test
    public void testTimeSeriesIteratorThrowsExceptionOnAttemptToMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        Iterator<TimeSeries<Double>> immutableTimeSeries = defaultTimeSeries.get().iterator();
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
        new BasicTimeSeriesBuilder<Double>().addTimeSeries( (TimeSeries<Double>) null ).build();

    }

}

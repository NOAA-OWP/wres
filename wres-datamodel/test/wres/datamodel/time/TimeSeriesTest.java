package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link TimeSeries}
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesTest
{

    /**
     * Events.
     */

    private SortedSet<Event<Double>> events;

    /**
     * A time-series.
     */

    private TimeSeries<Double> testSeries;

    /**
     * A reference time.
     */

    private Instant referenceTime;

    @Before
    public void runBeforeEachTest()
    {
        this.events = new TreeSet<>();

        this.events.add( Event.of( Instant.parse( "2123-12-01T06:00:00Z" ), 1.0 ) );
        this.events.add( Event.of( Instant.parse( "2123-12-01T12:00:00Z" ), 2.0 ) );
        this.events.add( Event.of( Instant.parse( "2123-12-01T18:00:00Z" ), 3.0 ) );

        this.referenceTime = Instant.parse( "2123-12-01T00:00:00Z" );

        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();

        Iterator<Event<Double>> iterator = events.iterator();

        testSeries = builder.setReferenceTime( this.referenceTime )
                            .setReferenceTimeType( ReferenceTimeType.T0 )
                            .addEvent( iterator.next() )
                            .addEvent( iterator.next() )
                            .addEvent( iterator.next() )
                            .build();
    }

    /**
     * Tests the {@link TimeSeries#getReferenceTime()}.
     */

    @Test
    public void testGetReferenceTime()
    {
        assertEquals( Instant.parse( "2123-12-01T00:00:00Z" ), testSeries.getReferenceTime() );
    }

    /**
     * Tests the {@link TimeSeries#getReferenceTimeType()}.
     */

    @Test
    public void testGetReferenceTimeType()
    {
        assertEquals( ReferenceTimeType.T0, this.testSeries.getReferenceTimeType() );
    }

    /**
     * Tests the {@link TimeSeries#getEvents()}.
     */

    @Test
    public void testGetEvents()
    {
        assertEquals( this.events, this.testSeries.getEvents() );
    }

    /**
     * Tests the {@link TimeSeries#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal objects have the same hashcode
        assertTrue( this.testSeries.equals( this.testSeries )
                    && this.testSeries.hashCode() == this.testSeries.hashCode() );

        // Consistent when invoked multiple times
        TimeSeries<Double> test = TimeSeries.of( this.referenceTime,
                                                   ReferenceTimeType.T0,
                                                   this.events );
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testSeries.hashCode(), test.hashCode() );
        }

    }

    /**
     * Tests {@link TimeSeries#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertTrue( this.testSeries.equals( this.testSeries ) );

        // Symmetric
        TimeSeries<Double> anotherTestSeries = TimeSeries.of( this.referenceTime,
                                                                ReferenceTimeType.T0,
                                                                this.events );

        assertTrue( anotherTestSeries.equals( this.testSeries ) && this.testSeries.equals( anotherTestSeries ) );

        // Transitive
        TimeSeries<Double> oneMoreTestSeries = TimeSeries.of( this.referenceTime,
                                                                ReferenceTimeType.T0,
                                                                this.events );

        assertTrue( this.testSeries.equals( anotherTestSeries ) && anotherTestSeries.equals( oneMoreTestSeries )
                    && this.testSeries.equals( oneMoreTestSeries ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( this.testSeries.equals( anotherTestSeries ) );
        }

        // Nullity
        assertNotEquals( null, anotherTestSeries );
        assertNotEquals( anotherTestSeries, null );

        // Check unequal cases
        TimeSeries<Double> unequalOnReferenceTime = TimeSeries.of( Instant.parse( "1990-03-01T12:00:00Z" ),
                                                                     ReferenceTimeType.T0,
                                                                     this.events );

        assertNotEquals( unequalOnReferenceTime, this.testSeries );

        TimeSeries<Double> unequalOnReferenceTimeType = TimeSeries.of( this.referenceTime,
                                                                         this.events );

        assertNotEquals( unequalOnReferenceTimeType, this.testSeries );

        SortedSet<Event<Double>> otherEvents = new TreeSet<>();
        otherEvents.add( Event.of( Instant.parse( "1985-01-06T12:00:00Z" ), 1.2 ) );

        TimeSeries<Double> unequalOnEvents = TimeSeries.of( this.referenceTime,
                                                              ReferenceTimeType.T0,
                                                              otherEvents );

        assertNotEquals( unequalOnEvents, this.testSeries );

        TimeSeries<Double> unequalOnMultiple = TimeSeries.of( otherEvents );

        assertNotEquals( unequalOnMultiple, this.testSeries );
    }

    /**
     * Asserts that a {@link TimeSeries} can have no events associated with it, i.e. can be empty.
     */
    @Test
    public void assertThatATimeSeriesCanBeEmpty()
    {
        assertEquals( Collections.emptySortedSet(), TimeSeries.of( Collections.emptySortedSet() ).getEvents() );
    }

    /**
     * Checks for an expected exception when attempting to add coincident events to the same time-series.
     */

    @Test
    public void testThatBuilderThrowsExpectedExceptionWhenAddingDuplicateTimes()
    {
        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();

        builder.addEvent( Event.of( Instant.MIN, 1.0 ) );

        IllegalArgumentException exception =
                assertThrows( IllegalArgumentException.class, () -> builder.addEvent( Event.of( Instant.MIN, 2.0 ) ) );

        assertEquals( "Attemped to add an event at the same valid datetime as an existing event, which is not allowed. "
                      + "The duplicate event time is '"
                      + Instant.MIN
                      + "'.",
                      exception.getMessage() );
    }

}

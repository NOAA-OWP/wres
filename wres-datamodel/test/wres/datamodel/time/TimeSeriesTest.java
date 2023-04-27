package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries.Builder;

import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link TimeSeries}
 * 
 * @author James Brown
 */

public class TimeSeriesTest
{

    private static final String VARIABLE_NAME = "Chickens";
    private static final Feature FEATURE_NAME = Feature.of(
            MessageFactory.getGeometry( "Georgia" ) );
    private static final String UNIT = "kg/h";

    /**
     * Events.
     */

    private SortedSet<Event<Double>> events;

    private TimeSeriesMetadata metadata;

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

        Builder<Double> builder = new Builder<>();

        Iterator<Event<Double>> iterator = events.iterator();

        this.metadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, this.referenceTime ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE_NAME,
                                       UNIT );
        this.testSeries = builder.setMetadata( metadata )
                            .addEvent( iterator.next() )
                            .addEvent( iterator.next() )
                            .addEvent( iterator.next() )
                            .build();
    }

    /**
     * Tests the {@link TimeSeries#getReferenceTimes()}.
     */

    @Test
    public void testGetReferenceTimes()
    {
        assertEquals( Collections.singletonMap( ReferenceTimeType.T0, Instant.parse( "2123-12-01T00:00:00Z" ) ),
                      testSeries.getReferenceTimes() );
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
     * Tests the {@link TimeSeries#getTimeScale()}.
     */

    @Test
    public void testGetTimeScale()
    {
        assertEquals( TimeScaleOuter.of(), this.testSeries.getTimeScale() );
    }

    /**
     * Tests the {@link TimeSeries#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.testSeries.hashCode(), this.testSeries.hashCode() );

        // Consistent when invoked multiple times
        TimeSeries<Double> test =
                new Builder<Double>().setMetadata( this.metadata )
                                     .addEvents( this.events )
                                     .build();
        TimeSeries.of( this.metadata,
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
        TimeSeries<Double> anotherTestSeries =
                new Builder<Double>().setMetadata( this.metadata )
                                     .addEvents( this.events )
                                     .build();

        assertTrue( anotherTestSeries.equals( this.testSeries ) && this.testSeries.equals( anotherTestSeries ) );

        // Transitive
        TimeSeries<Double> oneMoreTestSeries =
                new Builder<Double>().setMetadata( this.metadata )
                                     .addEvents( this.events )
                                     .build();

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
        TimeSeriesMetadata metadataWithOtherReferenceTime =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                               Instant.parse( "1990-03-01T12:00:00Z" ) ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE_NAME,
                                       UNIT );
        TimeSeries<Double> unequalOnReferenceTime = TimeSeries.of( metadataWithOtherReferenceTime,
                                                                   this.events );

        assertNotEquals( unequalOnReferenceTime, this.testSeries );

        TimeSeriesMetadata metadataWithOtherReferenceTimeType =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ISSUED_TIME,
                                               this.referenceTime ),
                                       TimeScaleOuter.of(),
                                       VARIABLE_NAME,
                                       FEATURE_NAME,
                                       UNIT );
        TimeSeries<Double> unequalOnReferenceTimeType = TimeSeries.of( metadataWithOtherReferenceTimeType,
                                                                       this.events );

        assertNotEquals( unequalOnReferenceTimeType, this.testSeries );

        SortedSet<Event<Double>> otherEvents = new TreeSet<>();
        otherEvents.add( Event.of( Instant.parse( "1985-01-06T12:00:00Z" ), 1.2 ) );

        TimeSeries<Double> unequalOnEvents = TimeSeries.of( this.metadata,
                                                            otherEvents );

        assertNotEquals( unequalOnEvents, this.testSeries );

        TimeSeries<Double> unequalOnMultiple = TimeSeries.of( metadataWithOtherReferenceTimeType,
                                                              otherEvents );

        assertNotEquals( unequalOnMultiple, this.testSeries );

        TimeSeries<Ensemble> theseEventValues =
                new Builder<Ensemble>().setMetadata( this.metadata )
                                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ),
                                                            Ensemble.of( 30.0, 65.0, 100.0 ) ) )
                                       .build();

        TimeSeries<Ensemble> doNotEqualThese =
                new Builder<Ensemble>().setMetadata( this.metadata )
                                       .addEvent( Event.of( Instant.parse( "2023-04-01T01:00:00Z" ),
                                                            Ensemble.of( 30.0, 65.0, 93.0 ) ) )
                                       .build();

        assertNotEquals( theseEventValues, doNotEqualThese );
    }

    /**
     * Asserts that a {@link TimeSeries} can have no events associated with it, i.e. can be empty.
     */
    @Test
    public void assertThatATimeSeriesCanBeEmpty()
    {
        assertEquals( Collections.emptySortedSet(),
                      TimeSeries.of( this.metadata,
                                     Collections.emptySortedSet() )
                                .getEvents() );
    }

    /**
     * Checks for an expected exception when attempting to add coincident events to the same time-series.
     */

    @Test
    public void testThatBuilderThrowsExpectedExceptionWhenAddingDuplicateTimes()
    {
        Builder<Double> builder = new Builder<>();

        builder.addEvent( Event.of( Instant.MIN, 1.0 ) );

        IllegalArgumentException exception =
                assertThrows( IllegalArgumentException.class, () -> builder.addEvent( Event.of( Instant.MIN, 2.0 ) ) );

        assertTrue( exception.getMessage().startsWith( "While building a time-series, attempted to add an event" ) );
    }

    /**
     * Tests the {@link TimeSeries#toString()}.
     */

    @Test
    public void testToString()
    {
        String actual = this.testSeries.toString();

        String expectedOne = "T0=2123-12-01T00:00:00Z";
        String expectedTwo = "(2123-12-01T06:00:00Z,1.0)";
        String expectedThree = "(2123-12-01T12:00:00Z,2.0)";
        String expectedFour = "(2123-12-01T18:00:00Z,3.0)";
        String expectedFive = "INSTANTANEOUS";

        assertTrue( actual.contains( expectedOne ) );
        assertTrue( actual.contains( expectedTwo ) );
        assertTrue( actual.contains( expectedThree ) );
        assertTrue( actual.contains( expectedFour ) );
        assertTrue( actual.contains( expectedFive ) );
    }
}

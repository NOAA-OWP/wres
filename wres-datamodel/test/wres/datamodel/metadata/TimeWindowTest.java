package wres.datamodel.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the {@link TimeWindow}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeWindowTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs an {@link TimeWindow} and tests for access to its immutable instance variables.
     */

    @Test
    public void testAccessors()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                           Instant.parse( "2009-12-31T11:59:59Z" ),
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 120 ) );
        assertEquals( window.getEarliestReferenceTime(), Instant.parse( "1985-01-01T00:00:00Z" ) );
        assertEquals( window.getLatestReferenceTime(), Instant.parse( "2010-12-31T11:59:59Z" ) );
        assertEquals( window.getEarliestValidTime(), Instant.parse( "1986-01-01T00:00:00Z" ) );
        assertEquals( window.getLatestValidTime(), Instant.parse( "2009-12-31T11:59:59Z" ) );
        assertEquals( window.getEarliestLeadDuration(), Duration.ofHours( 6 ) );
        assertEquals( window.getLatestLeadDuration(), Duration.ofHours( 120 ) );

        //Test mid-point of window 
        assertTrue( "Unexpected error in mid-point of time window.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "1985-01-10T00:00:00Z" ) )
                              .getMidPointBetweenEarliestAndLatestReferenceTimes()
                              .equals( Instant.parse( "1985-01-05T12:00:00Z" ) ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Duration.ZERO );
        TimeWindow equalWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "2010-12-31T11:59:59Z" ) );
        assertTrue( "Unexpected inequality between time windows.",
                    window.equals( equalWindow ) );
        assertTrue( "Unexpected equality between time windows on input type.",
                    !window.equals( new Double( 1.0 ) ) );
        assertTrue( "Unexpected equality between time windows on earliest time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:01Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ) ) ) );
        assertTrue( "Unexpected equality between time windows on latest time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2011-01-01T00:00:00Z" ) ) ) );
        assertTrue( "Unexpected equality between time windows on earliest valid time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.MAX,
                                                   Duration.ZERO,
                                                   Duration.ZERO ) ) );
        assertTrue( "Unexpected equality between time windows on latest valid time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   Instant.MIN,
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   Duration.ZERO,
                                                   Duration.ZERO ) ) );
        assertTrue( "Unexpected equality between time windows on earliest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   Duration.ofHours( -1 ),
                                                   Duration.ZERO ) ) );
        assertTrue( "Unexpected equality between time windows on latest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   Duration.ZERO,
                                                   Duration.ofHours( 1 ) ) ) );
        TimeWindow hours = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          Duration.ofHours( 1 ),
                                          Duration.ofHours( 1 ) );
        TimeWindow days = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                         Duration.ofDays( 1 ),
                                         Duration.ofDays( 1 ) );
        assertTrue( "Unexpected equality between time windows on lead time units.",
                    !hours.equals( days ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testHashCode()
    {
        TimeWindow first = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          Duration.ZERO );
        TimeWindow second = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ) );

        TimeWindow third = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          Instant.parse( "1986-01-01T00:00:00Z" ),
                                          Instant.parse( "2009-12-31T11:59:59Z" ),
                                          Duration.ZERO,
                                          Duration.ofHours( 120 ) );
        TimeWindow fourth = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                           Instant.parse( "2009-12-31T11:59:59Z" ),
                                           Duration.ZERO,
                                           Duration.ofHours( 120 ) );
        assertEquals( first.hashCode(), second.hashCode() );
        assertEquals( third.hashCode(), fourth.hashCode() );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#compareTo(TimeWindow)} against other instances.
     */

    @Test
    public void testCompareTo()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Duration.ZERO );

        //EQUAL
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ZERO ) ) == 0 );
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   Duration.ZERO )
                              .compareTo( window ) == 0 );
        //Transitive
        //x.compareTo(y) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ZERO ) ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   Duration.ZERO )
                              .compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         Duration.ZERO ) ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ZERO ) ) > 0 );
        //DIFFERENCES ON EARLIEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ZERO ) ) < 0 );
        //DIFFERENCES ON LATEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2011-12-31T11:59:59Z" ),
                                                     Duration.ZERO ) ) < 0 );

        //DIFFERENCES ON EARLIEST VALID TIME
        assertTrue( "Current window should have a different earliest valid time.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.MAX,
                                                     Duration.ZERO,
                                                     Duration.ZERO ) ) < 0 );

        //DIFFERENCES ON LATEST VALID TIME
        assertTrue( "Current window should have a different latest valid time.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Instant.MIN,
                                                     Instant.parse( "2011-12-31T11:59:59Z" ),
                                                     Duration.ZERO,
                                                     Duration.ZERO ) ) > 0 );

        //DIFFERENCES ON EARLIEST LEAD TIME
        assertTrue( "Current window should have an earliest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ofHours( 1 ) ) ) < 0 );
        //DIFFERENCES ON LATEST LEAD TIME
        assertTrue( "Current window should have a latest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ZERO,
                                                     Duration.ofHours( 1 ) ) ) < 0 );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#toString()} against other instances.
     */

    @Test
    public void testToString()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Duration.ZERO,
                                           Duration.ZERO );
        //Equality of strings for equal objects
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString().equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             Duration.ZERO,
                                                             Duration.ZERO )
                                                        .toString() ) );

        //Equality against a benchmark
        assertTrue( window.toString()
                          .equals( "[1985-01-01T00:00:00Z,2010-12-31T11:59:59Z,"
                                   + "1985-01-01T00:00:00Z,2010-12-31T11:59:59Z,PT0S,PT0S]" ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest reference time is 
     * after the latest reference time.
     */

    @Test
    public void testExceptionOnEarliestReferenceTimeAfterLatestReferenceTime()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot define a time window whose latest reference time is before its "
                + "earliest reference time." );

        TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "1984-12-31T11:59:59Z" ),
                       Duration.ZERO );
    }
    
    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest valid time is 
     * after the latest valid time.
     */

    @Test
    public void testExceptionOnEarliestValidTimeAfterLatestValidTime()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot define a time window whose latest valid time is before its "
                + "earliest valid time." );

        TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "1986-12-31T11:59:59Z" ),
                       Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "1984-12-31T11:59:59Z" ),
                       Duration.ZERO,
                       Duration.ZERO );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest lead time is after the latest 
     * lead time.
     */

    @Test
    public void testExceptionOnEarliestLeadTimeAfterLatestLeadTime()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot define a time window whose latest lead duration is before its earliest "
                                 + "lead duration." );

        TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "2010-12-31T11:59:59Z" ),
                       Duration.ofHours( 1 ),
                       Duration.ZERO );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest reference time is null.
     */

    @Test
    public void testExceptionWhenEarliestReferenceTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The earliest reference time cannot be null." );

        TimeWindow.of( null,
                       Instant.MAX,
                       Duration.ZERO );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the latest reference time is null.
     */

    @Test
    public void testExceptionWhenLatestReferenceTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The latest reference time cannot be null." );

        TimeWindow.of( Instant.MIN,
                       null,
                       Duration.ofHours( 1 ),
                       Duration.ZERO );
    }
    
    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest valid time is null.
     */

    @Test
    public void testExceptionWhenEarliestValidTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The earliest valid time cannot be null." );

        TimeWindow.of( Instant.MIN,
                       Instant.MAX,
                       null,
                       Instant.MAX,
                       Duration.ZERO,
                       Duration.ZERO );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the latest valid time is null.
     */

    @Test
    public void testExceptionWhenLatestValidTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The latest valid time cannot be null." );

        TimeWindow.of( Instant.MIN,
                       Instant.MAX,
                       Instant.MIN,
                       null,
                       Duration.ZERO,
                       Duration.ZERO );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest lead duration is null.
     */

    @Test
    public void testExceptionWhenEarliestLeadDurationIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The earliest lead duration cannot be null." );

        TimeWindow.of( Instant.MIN,
                       Instant.MAX,
                       Instant.MIN,
                       Instant.MAX,
                       null,
                       Duration.ZERO );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the latest lead duration is null.
     */

    @Test
    public void testExceptionWhenLatestLeadDurationTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The latest lead duration cannot be null." );

        TimeWindow.of( Instant.MIN,
                       Instant.MAX,
                       Instant.MIN,
                       Instant.MAX,
                       Duration.ZERO,
                       null );
    }
    
    /**
     * Tests {@link TimeWindow#hasUnboundedReferenceTimes()}.
     */

    @Test
    public void testHasUnboundedReferenceTimes()
    {
        TimeWindow bounded = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                            Instant.parse( "2015-12-31T11:59:59Z" ),
                                            Duration.ZERO );

        assertFalse( bounded.hasUnboundedReferenceTimes() );

        TimeWindow unbounded = TimeWindow.of( Instant.MIN,
                                              Instant.MAX,
                                              Duration.ZERO );
        assertTrue( unbounded.hasUnboundedReferenceTimes() );

        TimeWindow partlyLow = TimeWindow.of( Instant.MIN,
                                              Instant.parse( "2015-12-31T11:59:59Z" ),
                                              Duration.ZERO );

        assertTrue( partlyLow.hasUnboundedReferenceTimes() );

        TimeWindow partlyHigh = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.MAX,
                                               Duration.ZERO );

        assertTrue( partlyHigh.hasUnboundedReferenceTimes() );
    }

    /**
     * Tests the {@link TimeWindow#unionWith(TimeWindow)}.
     */

    @Test
    public void testUnionWith()
    {
        TimeWindow first = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2017-12-31T11:59:59Z" ),
                                          Duration.ofHours( 5 ),
                                          Duration.ofHours( 25 ) );
        TimeWindow second = TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                           Instant.parse( "2015-12-31T11:59:59Z" ),
                                           Duration.ofHours( -5 ),
                                           Duration.ofHours( 20 ) );
        TimeWindow expected = TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                             Instant.parse( "2017-12-31T11:59:59Z" ),
                                             Duration.ofHours( -5 ),
                                             Duration.ofHours( 25 ) );
        List<TimeWindow> union = new ArrayList<>();
        union.add( first );
        union.add( second );

        assertTrue( TimeWindow.unionOf( union ).equals( expected ) );


        TimeWindow third = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2017-12-31T11:59:59Z" ),
                                          Instant.parse( "1983-01-01T00:00:00Z" ),
                                          Instant.parse( "2019-12-31T11:59:59Z" ),
                                          Duration.ofHours( 5 ),
                                          Duration.ofHours( 21 ) );
        TimeWindow fourth = TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                           Instant.parse( "2015-12-31T11:59:59Z" ),
                                           Instant.parse( "1982-01-01T00:00:00Z" ),
                                           Instant.parse( "2017-12-31T11:59:59Z" ),
                                           Duration.ZERO,
                                           Duration.ofHours( 20 ) );
        TimeWindow expectedTwo = TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                Instant.parse( "2017-12-31T11:59:59Z" ),
                                                Instant.parse( "1982-01-01T00:00:00Z" ),
                                                Instant.parse( "2019-12-31T11:59:59Z" ),
                                                Duration.ZERO,
                                                Duration.ofHours( 21 ) );
        List<TimeWindow> unionTwo = new ArrayList<>();
        unionTwo.add( third );
        unionTwo.add( fourth );

        assertTrue( TimeWindow.unionOf( unionTwo ).equals( expectedTwo ) );

    }

}

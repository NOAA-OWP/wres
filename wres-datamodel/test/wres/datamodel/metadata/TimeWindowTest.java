package wres.datamodel.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 120 ) );
        assertTrue( "Unexpected start date in time window.",
                    window.getEarliestTime().equals( Instant.parse( "1985-01-01T00:00:00Z" ) ) );
        assertTrue( "Unexpected end date in time window.",
                    window.getLatestTime().equals( Instant.parse( "2010-12-31T11:59:59Z" ) ) );
        assertTrue( "Unexpected reference time system in window window.",
                    window.getReferenceTime().equals( ReferenceTime.VALID_TIME ) );
        assertTrue( "Unexpected earliest lead time in time window.",
                    window.getEarliestLeadTime().compareTo( Duration.ofHours( 6 ) ) == 0 );
        assertTrue( "Unexpected latest lead time in time window.",
                    window.getLatestLeadTime().compareTo( Duration.ofHours( 120 ) ) == 0 );

        //Test mid-point of window 
        assertTrue( "Unexpected error in mid-point of time window.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "1985-01-10T00:00:00Z" ) )
                              .getMidPointBetweenEarliestAndLatestTimes()
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
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 0 ) );
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
        assertTrue( "Unexpected equality between time windows on reference time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   ReferenceTime.ISSUE_TIME,
                                                   Duration.ofHours( 0 ) ) ) );
        assertTrue( "Unexpected equality between time windows on earliest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( -1 ),
                                                   Duration.ofHours( 0 ) ) ) );
        assertTrue( "Unexpected equality between time windows on latest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   ReferenceTime.VALID_TIME,
                                                   Duration.ofHours( 0 ),
                                                   Duration.ofHours( 1 ) ) ) );
        TimeWindow hours = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          Duration.ofHours( 1 ),
                                          Duration.ofHours( 1 ) );
        TimeWindow days = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                         ReferenceTime.VALID_TIME,
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
                                          ReferenceTime.VALID_TIME,
                                          Duration.ofHours( 0 ) );
        TimeWindow second = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ) );

        TimeWindow third = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          Duration.ofHours( 0 ),
                                          Duration.ofHours( 120 ) );
        TimeWindow fourth = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 0 ),
                                           Duration.ofHours( 120 ) );
        assertEquals( "Unexpected hash inequality between time windows.", first.hashCode(), second.hashCode() );
        assertEquals( "Unexpected hash inequality between time windows.", third.hashCode(), fourth.hashCode() );
        assertTrue( "Unexpected hash equality between time windows on input type.",
                    first.hashCode() != Double.hashCode( 1.0 ) );
        assertTrue( "Unexpected hash equality between time windows on earliest time.",
                    first.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:01Z" ),
                                                                         Instant.parse( "2010-12-31T11:59:59Z" ) ) ) );
        assertTrue( "Unexpected hash equality between time windows on latest time.",
                    first.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "2011-01-01T00:00:00Z" ) ) ) );
        assertTrue( "Unexpected hash equality between time windows on reference time.",
                    first.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                         ReferenceTime.ISSUE_TIME,
                                                                         Duration.ofHours( 0 ) ) ) );
        assertTrue( "Unexpected hash equality between time windows on earliest lead time.",
                    first.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                         ReferenceTime.VALID_TIME,
                                                                         Duration.ofHours( -10 ),
                                                                         Duration.ofHours( 0 ) ) ) );
        TimeWindow hours = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          Duration.ofHours( 0 ),
                                          Duration.ofHours( 1 ) );
        assertTrue( "Unexpected hash equality between time windows on latest lead time.",
                    first.hashCode() != hours.hashCode() );
        TimeWindow days = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                         ReferenceTime.VALID_TIME,
                                         Duration.ofDays( 0 ),
                                         Duration.ofDays( 1 ) );
        assertTrue( "Unexpected hash equality between time windows on time units.",
                    days.hashCode() != hours.hashCode() );
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
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 0 ) );

        //EQUAL
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 0 ) ) ) == 0 );
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   ReferenceTime.VALID_TIME,
                                   Duration.ofHours( 0 ) )
                              .compareTo( window ) == 0 );
        //DIFFERENCES ON EARLIEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 0 ) ) ) < 0 );
        //Transitive
        //x.compareTo(y) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 0 ) ) ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   ReferenceTime.VALID_TIME,
                                   Duration.ofHours( 0 ) )
                              .compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( 0 ) ) ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 0 ) ) ) > 0 );

        //DIFFERENCES ON LATEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2011-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 0 ) ) ) < 0 );
        //DIFFERENCES ON REFERENCE TIME
        assertTrue( "Current window should have a different reference time system.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.ISSUE_TIME,
                                                     Duration.ofHours( 0 ) ) ) < 0 );
        //DIFFERENCES ON EARLIEST LEAD TIME
        assertTrue( "Current window should have an earliest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 1 ) ) ) < 0 );
        //DIFFERENCES ON LATEST LEAD TIME
        assertTrue( "Current window should have a latest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 0 ),
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
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 0 ) );
        //Equality of strings for equal objects
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString().equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             ReferenceTime.VALID_TIME,
                                                             Duration.ofHours( 0 ) )
                                                        .toString() ) );

        //Equality against a benchmark
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString()
                          .equals( "[1985-01-01T00:00:00Z,2010-12-31T11:59:59Z,VALID TIME,PT0S,PT0S]" ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest time is after the latest time.
     */

    @Test
    public void testExceptionOnEarliestTimeAfterLatestTime()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot define a time window whose latest time is before its earliest time." );

        TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "1984-12-31T11:59:59Z" ),
                       ReferenceTime.VALID_TIME,
                       Duration.ofHours( 0 ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest lead time is after the latest 
     * lead time.
     */

    @Test
    public void testExceptionOnEarliestLeadTimeAfterLatestLeadTime()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot define a time window whose latest lead time is before its earliest "
                + "lead time." );

        TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "2010-12-31T11:59:59Z" ),
                       ReferenceTime.VALID_TIME,
                       Duration.ofHours( 1 ),
                       Duration.ofHours( 0 ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the earliest time is null.
     */

    @Test
    public void testExceptionWhenEarliestTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The earliest time cannot be null." );

        TimeWindow.of( null,
                       Instant.parse( "1984-12-31T11:59:59Z" ),
                       ReferenceTime.VALID_TIME,
                       Duration.ofHours( 0 ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for an exception when the latest time is null.
     */

    @Test
    public void testExceptionWhenLatestTimeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The latest time cannot be null." );

        TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       null,
                       ReferenceTime.VALID_TIME,
                       Duration.ofHours( 1 ),
                       Duration.ofHours( 0 ) );
    }
    
    /**
     * Tests {@link TimeWindow#hasUnboundedTimes()}.
     */

    @Test
    public void testHasUnboundedTimes()
    {
        TimeWindow bounded = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                            Instant.parse( "2015-12-31T11:59:59Z" ),
                                            ReferenceTime.VALID_TIME,
                                            Duration.ofHours( 0 ) );
        TimeWindow unbounded = TimeWindow.of( Instant.MIN,
                                              Instant.MAX,
                                              ReferenceTime.VALID_TIME,
                                              Duration.ofHours( 0 ) );
        TimeWindow partlyLow = TimeWindow.of( Instant.MIN,
                                              Instant.parse( "2015-12-31T11:59:59Z" ),
                                              ReferenceTime.VALID_TIME,
                                              Duration.ofHours( 0 ) );

        TimeWindow partlyHigh = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.MAX,
                                               ReferenceTime.VALID_TIME,
                                               Duration.ofHours( 0 ) );
        assertFalse( "Expected bounded time window.", bounded.hasUnboundedTimes() );
        assertTrue( "Expected unbounded time window on both bounds.", unbounded.hasUnboundedTimes() );
        assertTrue( "Expected unbounded time window on the low bound.", partlyLow.hasUnboundedTimes() );
        assertTrue( "Expected unbounded time window on the high bound.", partlyHigh.hasUnboundedTimes() );
    }

    /**
     * Tests the {@link TimeWindow#unionWith(TimeWindow)}.
     */

    @Test
    public void testUnionWith()
    {
        TimeWindow first = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2017-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          Duration.ofHours( 5 ),
                                          Duration.ofHours( 25 ) );
        TimeWindow second = TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                           Instant.parse( "2015-12-31T11:59:59Z" ),
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( -5 ),
                                           Duration.ofHours( 20 ) );
        TimeWindow expected = TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                             Instant.parse( "2017-12-31T11:59:59Z" ),
                                             ReferenceTime.VALID_TIME,
                                             Duration.ofHours( -5 ),
                                             Duration.ofHours( 25 ) );
        List<TimeWindow> union = new ArrayList<>();
        union.add( first );
        union.add( second );
        assertTrue( "Unexpected union of two time windows.", TimeWindow.unionOf( union ).equals( expected ) );
    }

}

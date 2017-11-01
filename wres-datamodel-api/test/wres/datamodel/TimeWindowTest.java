package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Objects;

import org.junit.Test;

/**
 * Tests the {@link TimeWindow}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class TimeWindowTest
{

    /**
     * Constructs an {@link TimeWindow} and tests for access to its immutable instance variables.
     */

    @Test
    public void test1TestAccessors()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           true,
                                           6 * 60 * 60,
                                           120 * 60 * 60 );
        assertTrue( "Unexpected start date in time window.",
                    window.getEarliestTime().equals( Instant.parse( "1985-01-01T00:00:00Z" ) ) );
        assertTrue( "Unexpected end date in time window.",
                    window.getLatestTime().equals( Instant.parse( "2010-12-31T11:59:59Z" ) ) );
        assertTrue( "Unexpected earliest lead time in time window.",
                    window.getEarliestLeadTimeInSeconds() == 6 * 60 * 60 );
        assertTrue( "Unexpected latest lead time in time window.",
                    window.getLatestLeadTimeInSeconds() == 120 * 60 * 60 );
        assertTrue( "Unexpected reference time system in window window.",
                    window.isValidTime() );
        assertTrue( "Unexpected earliest lead time (decimal hours) in time window.",
                    Double.compare( window.getEarliestLeadTimeInDecimalHours(), 6.0 ) == 0 );
        assertTrue( "Unexpected latest lead time (decimal hours) in time window.",
                    Double.compare( window.getLatestLeadTimeInDecimalHours(), 120.0 ) == 0 );
        //Test mid-point of window 
        assertTrue( "Unexpected error in mid-point of time window.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "1985-01-10T00:00:00Z" ),
                                   true )
                              .getMidPointTime()
                              .equals( Instant.parse( "1985-01-05T12:00:00Z" ) ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#equals(Object)} against other instances.
     */

    @Test
    public void test2Equals()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           true,
                                           0,
                                           0 );
        TimeWindow equalWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "2010-12-31T11:59:59Z" ),
                                                true );
        assertTrue( "Unexpected inequality between time windows.",
                    window.equals( equalWindow ) );
        assertTrue( "Unexpected equality between time windows on input type.",
                    !window.equals( new Double( 1.0 ) ) );
        assertTrue( "Unexpected equality between time windows on earliest time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:01Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   true ) ) );
        assertTrue( "Unexpected equality between time windows on latest time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2011-01-01T00:00:00Z" ),
                                                   true ) ) );
        assertTrue( "Unexpected equality between time windows on reference time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   false ) ) );
        assertTrue( "Unexpected equality between time windows on earliest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   true,
                                                   -1,
                                                   0 ) ) );
        assertTrue( "Unexpected equality between time windows on latest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   true,
                                                   0,
                                                   1 ) ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#hashCode()} against the hashes of other instances.
     */

    @Test
    public void test3HashCode()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           true,
                                           0,
                                           0 );
        TimeWindow equalWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "2010-12-31T11:59:59Z" ),
                                                true );
        assertTrue( "Unexpected hash inequality between time windows.",
                    window.hashCode() == equalWindow.hashCode() );
        assertTrue( "Unexpected hash equality between time windows on input type.",
                    window.hashCode() != Double.hashCode( 1.0 ) );
        assertTrue( "Unexpected hash equality between time windows on earliest time.",
                    window.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:01Z" ),
                                                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                          true ) ) );
        assertTrue( "Unexpected hash equality between time windows on latest time.",
                    window.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "2011-01-01T00:00:00Z" ),
                                                                          true ) ) );
        assertTrue( "Unexpected hash equality between time windows on reference time.",
                    window.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                          false ) ) );
        assertTrue( "Unexpected hash equality between time windows on earliest lead time.",
                    window.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                          true,
                                                                          -1,
                                                                          0 ) ) );
        assertTrue( "Unexpected hash equality between time windows on latest lead time.",
                    window.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                          true,
                                                                          0,
                                                                          1 ) ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#compareTo(TimeWindow)} against other instances.
     */

    @Test
    public void test4CompareTo()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           true,
                                           0,
                                           0 );

        //EQUAL
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     true,
                                                     0,
                                                     0 ) ) == 0 );
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   true,
                                   0,
                                   0 )
                              .compareTo( window ) == 0 );
        //DIFFERENCES ON EARLIEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     true,
                                                     0,
                                                     0 ) ) < 0 );
        //Transitive
        //x.compareTo(y) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     true,
                                                     0,
                                                     0 ) ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   true,
                                   0,
                                   0 )
                              .compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         true,
                                                         0,
                                                         0 ) ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     true,
                                                     0,
                                                     0 ) ) > 0 );

        //DIFFERENCES ON LATEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2011-12-31T11:59:59Z" ),
                                                     true,
                                                     0,
                                                     0 ) ) < 0 );
        //DIFFERENCES ON REFERENCE TIME
        assertTrue( "Current window should have a different reference time system.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     false ) ) > 0 );
        //DIFFERENCES ON EARLIEST LEAD TIME
        assertTrue( "Current window should have an earliest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     true,
                                                     1,
                                                     1 ) ) < 0 );
        //DIFFERENCES ON LATEST LEAD TIME
        assertTrue( "Current window should have a latest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     true,
                                                     0,
                                                     1 ) ) < 0 );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#toString()} against other instances.
     */

    @Test
    public void test5ToString()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           true,
                                           0,
                                           0 );
        //Equality of strings for equal objects
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString().equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             true,
                                                             0,
                                                             0 )
                                                        .toString() ) );
        //Equality against a benchmark
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString().equals( "[1985-01-01T00:00Z, 2010-12-31T11:59:59Z, true, 0, 0]" ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests for exceptions.
     */

    @Test
    public void test5Exceptions()
    {
        //Start time earlier than end time
        try
        {
            TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                           Instant.parse( "1984-12-31T11:59:59Z" ),
                           true,
                           0,
                           0 );
            fail( "Expected an exception on an end time that is before the start time." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        //Earliest lead time later than latest lead time
        try
        {
            TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                           Instant.parse( "2010-12-31T11:59:59Z" ),
                           true,
                           1,
                           0 );
            fail( "Expected an exception on a latest lead time that is before the earliest lead time." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }


}

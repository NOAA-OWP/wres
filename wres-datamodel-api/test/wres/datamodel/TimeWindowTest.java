package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import org.junit.Test;

import wres.datamodel.time.ReferenceTime;
import wres.datamodel.time.TimeWindow;

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
                                           ReferenceTime.VALID_TIME,
                                           6,
                                           120,
                                           ChronoUnit.HOURS );
        assertTrue( "Unexpected start date in time window.",
                    window.getEarliestTime().equals( Instant.parse( "1985-01-01T00:00:00Z" ) ) );
        assertTrue( "Unexpected end date in time window.",
                    window.getLatestTime().equals( Instant.parse( "2010-12-31T11:59:59Z" ) ) );
        assertTrue( "Unexpected earliest lead time in time window.",
                    window.getEarliestLeadTimeInHours() == 6 );
        assertTrue( "Unexpected latest lead time in time window.",
                    window.getLatestLeadTimeInHours() == 120 );
        assertTrue( "Unexpected reference time system in window window.",
                    window.getReferenceTime().equals( ReferenceTime.VALID_TIME ) );
        assertTrue( "Unexpected earliest lead time (hours) in time window.",
                    Long.compare( window.getEarliestLeadTimeInHours(), 6 ) == 0 );
        assertTrue( "Unexpected latest lead time (hours) in time window.",
                    Long.compare( window.getLatestLeadTimeInHours(), 120 ) == 0 );
        assertTrue( "Unexpected earliest lead time (seconds) in time window.",
                    Long.compare( window.getEarliestLeadTimeInSeconds(), 6 * 60 * 60) == 0 );
        assertTrue( "Unexpected latest lead time (seconds) in time window.",
                    Long.compare( window.getLatestLeadTimeInSeconds(), 120 * 60 * 60 ) == 0 );       
        assertTrue( "Unexpected earliest lead time in time window.",
                    Long.compare( window.getEarliestLeadTime(), 6 ) == 0 );
        assertTrue( "Unexpected latest lead time in time window.",
                    Long.compare( window.getLatestLeadTime(), 120 ) == 0 );
        assertTrue( "Unexpected lead time units in time window.",
                    window.getLeadUnits() == ChronoUnit.HOURS );
        

        //Test mid-point of window 
        assertTrue( "Unexpected error in mid-point of time window.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "1985-01-10T00:00:00Z" ) )
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
                                           ReferenceTime.VALID_TIME,
                                           0 );
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
                                                   0 ) ) );
        assertTrue( "Unexpected equality between time windows on earliest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   ReferenceTime.VALID_TIME,
                                                   -1,
                                                   0,
                                                   ChronoUnit.HOURS ) ) );
        assertTrue( "Unexpected equality between time windows on latest lead time.",
                    !window.equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                   ReferenceTime.VALID_TIME,
                                                   0,
                                                   1,
                                                   ChronoUnit.HOURS ) ) );
        TimeWindow hours = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "2010-12-31T11:59:59Z" ),
                       ReferenceTime.VALID_TIME,
                       1,
                       1,
                       ChronoUnit.HOURS ); 
        TimeWindow days = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          1,
                                          1,
                                          ChronoUnit.DAYS );                            
        assertTrue( "Unexpected equality between time windows on lead time units.",
                    !hours.equals( days ) );
    }

    /**
     * Constructs a {@link TimeWindow} and tests {@link TimeWindow#hashCode()} against the hashes of other instances.
     */

    @Test
    public void test3HashCode()
    {
        TimeWindow first = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           ReferenceTime.VALID_TIME,
                                           0 );
        TimeWindow second = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "2010-12-31T11:59:59Z" ) );
        
        TimeWindow third = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                       Instant.parse( "2010-12-31T11:59:59Z" ),
                       ReferenceTime.VALID_TIME,
                       0,
                       120,
                       ChronoUnit.HOURS );
        TimeWindow fourth = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          0,
                                          120,
                                          ChronoUnit.HOURS );
        assertEquals( "Unexpected hash inequality between time windows.",first.hashCode(), second.hashCode() );
        assertEquals( "Unexpected hash inequality between time windows.",third.hashCode(), fourth.hashCode() );
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
                                                                          0 ) ) );
        assertTrue( "Unexpected hash equality between time windows on earliest lead time.",
                    first.hashCode() != Objects.hashCode( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                          ReferenceTime.VALID_TIME,
                                                                          -10,
                                                                          0,
                                                                          ChronoUnit.HOURS ) ) );
        TimeWindow hours = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                            Instant.parse( "2010-12-31T11:59:59Z" ),
                                                            ReferenceTime.VALID_TIME,
                                                            0,
                                                            1,
                                                            ChronoUnit.HOURS );
        assertTrue( "Unexpected hash equality between time windows on latest lead time.",
                    first.hashCode() != hours.hashCode() );
        TimeWindow days = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                          Instant.parse( "2010-12-31T11:59:59Z" ),
                                          ReferenceTime.VALID_TIME,
                                          0,
                                          1,
                                          ChronoUnit.DAYS );
        assertTrue( "Unexpected hash equality between time windows on time units.",
                    days.hashCode() != hours.hashCode() );
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
                                           ReferenceTime.VALID_TIME,
                                           0 );

        //EQUAL
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     0 ) ) == 0 );
        assertTrue( "Unexpected inequality between two time windows that are equal.",
                    TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   ReferenceTime.VALID_TIME,
                                   0 )
                              .compareTo( window ) == 0 );
        //DIFFERENCES ON EARLIEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     0 ) ) < 0 );
        //Transitive
        //x.compareTo(y) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     0 ) ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    TimeWindow.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                   ReferenceTime.VALID_TIME,
                                   0 )
                              .compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         0 ) ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( "Current window should be later than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1983-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     0 ) ) > 0 );

        //DIFFERENCES ON LATEST TIME
        assertTrue( "Current window should be earlier than input window.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2011-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     0 ) ) < 0 );
        //DIFFERENCES ON REFERENCE TIME
        assertTrue( "Current window should have a different reference time system.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.ISSUE_TIME,
                                                     0 ) ) < 0 );
        //DIFFERENCES ON EARLIEST LEAD TIME
        assertTrue( "Current window should have an earliest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     1 ) ) < 0 );
        //DIFFERENCES ON LATEST LEAD TIME
        assertTrue( "Current window should have a latest lead time that is earlier.",
                    window.compareTo( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     0,
                                                     1,
                                                     ChronoUnit.HOURS ) ) < 0 );
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
                                           ReferenceTime.VALID_TIME,
                                           0 );
        //Equality of strings for equal objects
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString().equals( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                             Instant.parse( "2010-12-31T11:59:59Z" ),
                                                             ReferenceTime.VALID_TIME,
                                                             0 )
                                                        .toString() ) );
        //Equality against a benchmark
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString()
                          .equals( "[1985-01-01T00:00:00Z, 2010-12-31T11:59:59Z, VALID TIME, 0 Hours, 0 Hours]" ) );
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
                           ReferenceTime.VALID_TIME,
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
                           ReferenceTime.VALID_TIME,
                           1,
                           0,
                           ChronoUnit.HOURS );
            fail( "Expected an exception on a latest lead time that is before the earliest lead time." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }


}

package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.google.protobuf.Timestamp;

import wres.datamodel.messages.MessageFactory;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link TimeWindowOuter}.
 * 
 * @author James Brown
 */
public final class TimeWindowOuterTest
{

    private static final Instant SEVENTH_TIME = Instant.parse( "2017-12-31T11:59:59Z" );
    private static final Instant SIXTH_TIME = Instant.parse( "2015-12-31T11:59:59Z" );
    private static final Instant FIFTH_TIME = Instant.parse( "2010-12-31T11:59:59Z" );
    private static final Instant FOURTH_TIME = Instant.parse( "2009-12-31T11:59:59Z" );
    private static final Instant THIRD_TIME = Instant.parse( "1986-01-01T00:00:00Z" );
    private static final Instant SECOND_TIME = Instant.parse( "1985-01-01T00:00:00Z" );
    private static final Instant FIRST_TIME = Instant.parse( "1983-01-01T00:00:00Z" );

    /**
     * Constructs an {@link TimeWindowOuter} and tests for access to its immutable instance variables.
     */

    @Test
    public void testAccessors()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow inner = MessageFactory.getTimeWindow( SECOND_TIME,
                                                         FIFTH_TIME,
                                                         THIRD_TIME,
                                                         FOURTH_TIME,
                                                         Duration.ofHours( 6 ),
                                                         Duration.ofHours( 120 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );
        assertEquals( SECOND_TIME, window.getEarliestReferenceTime() );
        assertEquals( FIFTH_TIME, window.getLatestReferenceTime() );
        assertEquals( THIRD_TIME, window.getEarliestValidTime() );
        assertEquals( FOURTH_TIME, window.getLatestValidTime() );
        assertEquals( Duration.ofHours( 6 ), window.getEarliestLeadDuration() );
        assertEquals( Duration.ofHours( 120 ), window.getLatestLeadDuration() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        TimeWindow inner = MessageFactory.getTimeWindow( SECOND_TIME,
                                                         FIFTH_TIME,
                                                         Duration.ofSeconds( Long.MIN_VALUE ),
                                                         Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        TimeWindow innerEqual = MessageFactory.getTimeWindow( SECOND_TIME,
                                                              FIFTH_TIME,
                                                              TimeWindowOuter.DURATION_MIN,
                                                              TimeWindowOuter.DURATION_MAX );

        TimeWindowOuter equalWindow = TimeWindowOuter.of( innerEqual );
        assertEquals( window, equalWindow );
        assertNotEquals( Double.valueOf( 1.0 ), window );
        assertNotEquals( window,
                         TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( "1985-01-01T00:00:01Z" ),
                                                                           FIFTH_TIME ) ) );
        assertNotEquals( window,
                         TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                           Instant.parse( "2011-01-01T00:00:00Z" ) ) ) );
        assertNotEquals( window,
                         TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                           FIFTH_TIME,
                                                                           SECOND_TIME,
                                                                           Instant.MAX,
                                                                           Duration.ZERO,
                                                                           Duration.ZERO ) ) );
        assertNotEquals( window,
                         TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                           FIFTH_TIME,
                                                                           Instant.MIN,
                                                                           FIFTH_TIME,
                                                                           Duration.ZERO,
                                                                           Duration.ZERO ) ) );
        assertNotEquals( window,
                         TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                           FIFTH_TIME,
                                                                           Duration.ofHours( -1 ),
                                                                           Duration.ZERO ) ) );
        assertNotEquals( window,
                         TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                           FIFTH_TIME,
                                                                           Duration.ZERO,
                                                                           Duration.ofHours( 1 ) ) ) );
        TimeWindowOuter hours = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                  FIFTH_TIME,
                                                                                  Duration.ofHours( 1 ),
                                                                                  Duration.ofHours( 1 ) ) );
        TimeWindowOuter days = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                 FIFTH_TIME,
                                                                                 Duration.ofDays( 1 ),
                                                                                 Duration.ofDays( 1 ) ) );
        assertNotEquals( hours, days );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testHashCode()
    {
        TimeWindowOuter first = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                  FIFTH_TIME,
                                                                                  Duration.ofSeconds( Long.MIN_VALUE ),
                                                                                  Duration.ofSeconds( Long.MAX_VALUE,
                                                                                                      999_999_999 ) ) );
        TimeWindowOuter second = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                   FIFTH_TIME,
                                                                                   TimeWindowOuter.DURATION_MIN,
                                                                                   TimeWindowOuter.DURATION_MAX ) );

        TimeWindowOuter third = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                  FIFTH_TIME,
                                                                                  THIRD_TIME,
                                                                                  FOURTH_TIME,
                                                                                  Duration.ZERO,
                                                                                  Duration.ofHours( 120 ) ) );
        TimeWindowOuter fourth = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                   FIFTH_TIME,
                                                                                   THIRD_TIME,
                                                                                   FOURTH_TIME,
                                                                                   Duration.ZERO,
                                                                                   Duration.ofHours( 120 ) ) );
        assertEquals( first.hashCode(), second.hashCode() );
        assertEquals( third.hashCode(), fourth.hashCode() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#compareTo(TimeWindowOuter)} against other instances.
     */

    @Test
    public void testCompareTo()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindowOuter window = TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                   FIFTH_TIME,
                                                                                   Duration.ZERO ) );

        //EQUAL
        assertEquals( 0,
                      window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                          FIFTH_TIME,
                                                                                          Duration.ZERO ) ) ) );
        assertEquals( 0,
                      TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                        FIFTH_TIME,
                                                                        Duration.ZERO ) )
                                     .compareTo( window ) );
        //Transitive
        //x.compareTo(y) > 0
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                                                        FIFTH_TIME,
                                                                                        Duration.ZERO ) ) ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                                      FIFTH_TIME,
                                                                      Duration.ZERO ) )
                                   .compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( FIRST_TIME,
                                                                                                 FIFTH_TIME,
                                                                                                 Duration.ZERO ) ) ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( FIRST_TIME,
                                                                                        FIFTH_TIME,
                                                                                        Duration.ZERO ) ) ) > 0 );
        //DIFFERENCES ON EARLIEST TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( THIRD_TIME,
                                                                                        FIFTH_TIME,
                                                                                        Duration.ZERO ) ) ) < 0 );
        //DIFFERENCES ON LATEST TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                        Instant.parse( "2011-12-31T11:59:59Z" ),
                                                                                        Duration.ZERO ) ) ) < 0 );

        //DIFFERENCES ON EARLIEST VALID TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                        FIFTH_TIME,
                                                                                        SECOND_TIME,
                                                                                        Instant.MAX,
                                                                                        Duration.ZERO,
                                                                                        Duration.ZERO ) ) ) < 0 );

        //DIFFERENCES ON LATEST VALID TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                        FIFTH_TIME,
                                                                                        Instant.MIN,
                                                                                        Instant.parse( "2011-12-31T11:59:59Z" ),
                                                                                        Duration.ZERO,
                                                                                        Duration.ZERO ) ) ) > 0 );

        //DIFFERENCES ON EARLIEST LEAD TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                        FIFTH_TIME,
                                                                                        Duration.ofHours( 1 ) ) ) ) < 0 );
        //DIFFERENCES ON LATEST LEAD TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                                        FIFTH_TIME,
                                                                                        Duration.ZERO,
                                                                                        Duration.ofHours( 1 ) ) ) ) < 0 );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#toString()} against other instances.
     */

    @Test
    public void testToString()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindow inner = MessageFactory.getTimeWindow( SECOND_TIME,
                                                         FIFTH_TIME,
                                                         SECOND_TIME,
                                                         FIFTH_TIME,
                                                         Duration.ZERO,
                                                         Duration.ZERO );
        TimeWindowOuter window = TimeWindowOuter.of( inner );
        //Equality of strings for equal objects
        assertEquals( TimeWindowOuter.of( MessageFactory.getTimeWindow( SECOND_TIME,
                                                                        FIFTH_TIME,
                                                                        SECOND_TIME,
                                                                        FIFTH_TIME,
                                                                        Duration.ZERO,
                                                                        Duration.ZERO ) )
                                     .toString(),
                      window.toString() );

        //Equality against a benchmark
        assertEquals( "TimeWindowOuter[earliestReferenceTime=1985-01-01T00:00:00Z,latestReferenceTime="
                      + "2010-12-31T11:59:59Z,earliestValidTime=1985-01-01T00:00:00Z,"
                      + "latestValidTime=2010-12-31T11:59:59Z,earliestLeadDuration=PT0S,latestLeadDuration=PT0S]",
                      window.toString() );
    }

    @Test
    public void testGetTimeWindow()
    {
        TimeWindow inner = MessageFactory.getTimeWindow( SECOND_TIME,
                                                         FIFTH_TIME,
                                                         Duration.ZERO );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        TimeWindow expected = TimeWindow.newBuilder()
                                        .setEarliestReferenceTime( Timestamp.newBuilder()
                                                                            .setSeconds( SECOND_TIME.getEpochSecond() ) )
                                        .setLatestReferenceTime( Timestamp.newBuilder()
                                                                          .setSeconds( FIFTH_TIME.getEpochSecond() ) )
                                        .setEarliestValidTime( Timestamp.newBuilder()
                                                                        .setSeconds( Instant.MIN.getEpochSecond() )
                                                                        .setNanos( Instant.MIN.getNano() ) )
                                        .setLatestValidTime( Timestamp.newBuilder()
                                                                      .setSeconds( Instant.MAX.getEpochSecond() )
                                                                      .setNanos( Instant.MAX.getNano() ) )
                                        .setEarliestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                              .setSeconds( 0 ) )
                                        .setLatestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                            .setSeconds( 0 ) )
                                        .build();

        assertEquals( expected, window.getTimeWindow() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the earliest reference time is 
     * after the latest reference time.
     */

    @Test
    public void testExceptionOnEarliestReferenceTimeAfterLatestReferenceTime()
    {
        TimeWindow timeWindow = MessageFactory.getTimeWindow( SECOND_TIME,
                                                              Instant.parse( "1984-12-31T11:59:59Z" ),
                                                              Duration.ZERO );
        IllegalArgumentException thrown = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeWindowOuter.of( timeWindow ) );
        assertEquals( "Cannot define a time window whose latest reference time is before its "
                      + "earliest reference time.",
                      thrown.getMessage() );

    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the earliest valid time is 
     * after the latest valid time.
     */

    @Test
    public void testExceptionOnEarliestValidTimeAfterLatestValidTime()
    {
        TimeWindow timeWindow = MessageFactory.getTimeWindow( SECOND_TIME,
                                                              Instant.parse( "1986-12-31T11:59:59Z" ),
                                                              SECOND_TIME,
                                                              Instant.parse( "1984-12-31T11:59:59Z" ),
                                                              Duration.ZERO,
                                                              Duration.ZERO );
        IllegalArgumentException thrown = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeWindowOuter.of( timeWindow ) );
        assertEquals( "Cannot define a time window whose latest valid time is before its earliest valid time.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the earliest lead time is after the latest 
     * lead time.
     */

    @Test
    public void testExceptionOnEarliestLeadTimeAfterLatestLeadTime()
    {
        TimeWindow timeWindow = MessageFactory.getTimeWindow( SECOND_TIME,
                                                              FIFTH_TIME,
                                                              Duration.ofHours( 1 ),
                                                              Duration.ZERO );
        IllegalArgumentException thrown = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeWindowOuter.of( timeWindow ) );
        assertEquals( "Cannot define a time window whose latest lead duration is before its earliest "
                      + "lead duration.",
                      thrown.getMessage() );
    }

    /**
     * Tests {@link TimeWindowOuter#hasUnboundedReferenceTimes()}.
     */

    @Test
    public void testHasUnboundedReferenceTimes()
    {
        TimeWindow innerBounded = MessageFactory.getTimeWindow( SECOND_TIME,
                                                                SIXTH_TIME,
                                                                Duration.ZERO );
        TimeWindowOuter bounded = TimeWindowOuter.of( innerBounded );

        assertFalse( bounded.hasUnboundedReferenceTimes() );

        TimeWindow innerUnbounded = MessageFactory.getTimeWindow();
        TimeWindowOuter unbounded = TimeWindowOuter.of( innerUnbounded );
        assertTrue( unbounded.hasUnboundedReferenceTimes() );

        TimeWindow innerBoundedLow = MessageFactory.getTimeWindow( Instant.MIN,
                                                                   SIXTH_TIME,
                                                                   Duration.ZERO );
        TimeWindowOuter partlyLow = TimeWindowOuter.of( innerBoundedLow );

        assertTrue( partlyLow.hasUnboundedReferenceTimes() );

        TimeWindow innerBoundedHigh = MessageFactory.getTimeWindow( SECOND_TIME,
                                                                    Instant.MAX,
                                                                    Duration.ZERO );
        TimeWindowOuter partlyHigh = TimeWindowOuter.of( innerBoundedHigh );

        assertTrue( partlyHigh.hasUnboundedReferenceTimes() );
    }

    /**
     * Tests {@link TimeWindowOuter#hasUnboundedValidTimes()}.
     */

    @Test
    public void testHasUnboundedValidTimes()
    {
        TimeWindow innerBounded = MessageFactory.getTimeWindow( FIRST_TIME,
                                                                SECOND_TIME,
                                                                THIRD_TIME,
                                                                FOURTH_TIME );
        TimeWindowOuter bounded = TimeWindowOuter.of( innerBounded );

        assertFalse( bounded.hasUnboundedValidTimes() );

        TimeWindow innerUnbounded = MessageFactory.getTimeWindow();
        TimeWindowOuter unbounded = TimeWindowOuter.of( innerUnbounded );

        assertTrue( unbounded.hasUnboundedValidTimes() );

        TimeWindow innerBoundedLow = MessageFactory.getTimeWindow( FIRST_TIME,
                                                                   SECOND_TIME,
                                                                   Instant.MIN,
                                                                   FOURTH_TIME );
        TimeWindowOuter partlyLow = TimeWindowOuter.of( innerBoundedLow );

        assertTrue( partlyLow.hasUnboundedValidTimes() );

        TimeWindow innerBoundedHigh = MessageFactory.getTimeWindow( FIRST_TIME,
                                                                    SECOND_TIME,
                                                                    THIRD_TIME,
                                                                    Instant.MAX );
        TimeWindowOuter partlyHigh = TimeWindowOuter.of( innerBoundedHigh );

        assertTrue( partlyHigh.hasUnboundedValidTimes() );
    }

    /**
     * Tests {@link TimeWindowOuter#bothLeadDurationsAreUnbounded()}.
     */

    @Test
    public void testBothLeadDurationsAreUnbounded()
    {
        TimeWindow innerBounded = MessageFactory.getTimeWindow( FIRST_TIME,
                                                                SECOND_TIME,
                                                                Duration.ZERO );
        TimeWindowOuter bounded = TimeWindowOuter.of( innerBounded );

        assertFalse( bounded.bothLeadDurationsAreUnbounded() );

        TimeWindow innerUnbounded = MessageFactory.getTimeWindow();
        TimeWindowOuter unbounded = TimeWindowOuter.of( innerUnbounded );

        assertTrue( unbounded.bothLeadDurationsAreUnbounded() );
    }

    /**
     * Tests the {@link TimeWindowOuter#unionOf(Set)}.
     */

    @Test
    public void testUnionOf()
    {
        TimeWindow firstInner = MessageFactory.getTimeWindow( SECOND_TIME,
                                                              SEVENTH_TIME,
                                                              Duration.ofHours( 5 ),
                                                              Duration.ofHours( 25 ) );
        TimeWindowOuter first = TimeWindowOuter.of( firstInner );
        TimeWindow secondInner = MessageFactory.getTimeWindow( FIRST_TIME,
                                                               SIXTH_TIME,
                                                               Duration.ofHours( -5 ),
                                                               Duration.ofHours( 20 ) );
        TimeWindowOuter second = TimeWindowOuter.of( secondInner );
        TimeWindow expectedInner = MessageFactory.getTimeWindow( FIRST_TIME,
                                                                 SEVENTH_TIME,
                                                                 Duration.ofHours( -5 ),
                                                                 Duration.ofHours( 25 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( expectedInner );
        Set<TimeWindowOuter> union = new HashSet<>();
        union.add( first );
        union.add( second );

        TimeWindowOuter actual = TimeWindowOuter.unionOf( union );

        assertEquals( expected, actual );

        TimeWindow thirdInner = MessageFactory.getTimeWindow( SECOND_TIME,
                                                              SEVENTH_TIME,
                                                              FIRST_TIME,
                                                              Instant.parse( "2019-12-31T11:59:59Z" ),
                                                              Duration.ofHours( 5 ),
                                                              Duration.ofHours( 21 ) );
        TimeWindowOuter third = TimeWindowOuter.of( thirdInner );
        TimeWindow fourthInner = MessageFactory.getTimeWindow( FIRST_TIME,
                                                               SIXTH_TIME,
                                                               Instant.parse( "1982-01-01T00:00:00Z" ),
                                                               SEVENTH_TIME,
                                                               Duration.ZERO,
                                                               Duration.ofHours( 20 ) );
        TimeWindowOuter fourth = TimeWindowOuter.of( fourthInner );
        TimeWindow fifthInner = MessageFactory.getTimeWindow( FIRST_TIME,
                                                              SEVENTH_TIME,
                                                              Instant.parse( "1982-01-01T00:00:00Z" ),
                                                              Instant.parse( "2019-12-31T11:59:59Z" ),
                                                              Duration.ZERO,
                                                              Duration.ofHours( 21 ) );
        TimeWindowOuter expectedTwo = TimeWindowOuter.of( fifthInner );
        Set<TimeWindowOuter> unionTwo = new HashSet<>();
        unionTwo.add( third );
        unionTwo.add( fourth );

        TimeWindowOuter actualTwo = TimeWindowOuter.unionOf( unionTwo );

        assertEquals( expectedTwo, actualTwo );
    }

    /**
     * Tests that {@link TimeWindowOuter#unionWith(TimeWindowOuter)} throws an {@link IllegalArgumentException} on empty input.
     */

    @Test
    public void testUnionWithThrowsExceptionOnEmptyInput()
    {
        Set<TimeWindowOuter> emptySet = Set.of();
        IllegalArgumentException thrown =
                assertThrows( IllegalArgumentException.class,
                              () -> TimeWindowOuter.unionOf( emptySet ) );

        assertEquals( "Cannot determine the union of time windows for empty input.", thrown.getMessage() );
    }

    /**
     * Tests that {@link TimeWindowOuter#unionWith(TimeWindowOuter)} throws an {@link IllegalArgumentException} on empty that
     * contains a <code>null</code>.
     */

    @Test
    public void testUnionWithThrowsExceptionOnInputWithNull()
    {

        Set<TimeWindowOuter> nullInput = new HashSet<>();
        nullInput.add( null );

        IllegalArgumentException thrown =
                assertThrows( IllegalArgumentException.class,
                              () -> TimeWindowOuter.unionOf( nullInput ) );

        assertEquals( "Cannot determine the union of time windows for input that contains one or more "
                      + "null time windows.",
                      thrown.getMessage() );
    }

    /**
     * Tests that {@link TimeWindowOuter#unionWith(TimeWindowOuter)} throws an {@link NullPointerException} on null input.
     */

    @Test
    public void testUnionWithThrowsExceptionOnNullInput()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class, () -> TimeWindowOuter.unionOf( null ) );

        assertEquals( "Cannot determine the union of time windows for a null input.", thrown.getMessage() );
    }

}

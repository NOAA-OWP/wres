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

import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link TimeWindowOuter}.
 * 
 * @author james.brown@hydrosolved.com
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
        TimeWindowOuter window = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     THIRD_TIME,
                                                     FOURTH_TIME,
                                                     Duration.ofHours( 6 ),
                                                     Duration.ofHours( 120 ) );
        assertEquals( window.getEarliestReferenceTime(), SECOND_TIME );
        assertEquals( window.getLatestReferenceTime(), FIFTH_TIME );
        assertEquals( window.getEarliestValidTime(), THIRD_TIME );
        assertEquals( window.getLatestValidTime(), FOURTH_TIME );
        assertEquals( window.getEarliestLeadDuration(), Duration.ofHours( 6 ) );
        assertEquals( window.getLatestLeadDuration(), Duration.ofHours( 120 ) );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        TimeWindowOuter window = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     Duration.ofSeconds( Long.MIN_VALUE ),
                                                     Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 ) );

        TimeWindowOuter equalWindow = TimeWindowOuter.of( SECOND_TIME,
                                                          FIFTH_TIME,
                                                          TimeWindowOuter.DURATION_MIN,
                                                          TimeWindowOuter.DURATION_MAX );
        assertEquals( window, equalWindow );
        assertNotEquals( Double.valueOf( 1.0 ), window );
        assertTrue( !window.equals( TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:01Z" ),
                                                        FIFTH_TIME ) ) );
        assertTrue( !window.equals( TimeWindowOuter.of( SECOND_TIME,
                                                        Instant.parse( "2011-01-01T00:00:00Z" ) ) ) );
        assertTrue( !window.equals( TimeWindowOuter.of( SECOND_TIME,
                                                        FIFTH_TIME,
                                                        SECOND_TIME,
                                                        Instant.MAX,
                                                        Duration.ZERO,
                                                        Duration.ZERO ) ) );
        assertTrue( !window.equals( TimeWindowOuter.of( SECOND_TIME,
                                                        FIFTH_TIME,
                                                        Instant.MIN,
                                                        FIFTH_TIME,
                                                        Duration.ZERO,
                                                        Duration.ZERO ) ) );
        assertTrue( !window.equals( TimeWindowOuter.of( SECOND_TIME,
                                                        FIFTH_TIME,
                                                        Duration.ofHours( -1 ),
                                                        Duration.ZERO ) ) );
        assertTrue( !window.equals( TimeWindowOuter.of( SECOND_TIME,
                                                        FIFTH_TIME,
                                                        Duration.ZERO,
                                                        Duration.ofHours( 1 ) ) ) );
        TimeWindowOuter hours = TimeWindowOuter.of( SECOND_TIME,
                                                    FIFTH_TIME,
                                                    Duration.ofHours( 1 ),
                                                    Duration.ofHours( 1 ) );
        TimeWindowOuter days = TimeWindowOuter.of( SECOND_TIME,
                                                   FIFTH_TIME,
                                                   Duration.ofDays( 1 ),
                                                   Duration.ofDays( 1 ) );
        assertTrue( !hours.equals( days ) );
    }

    @Test
    public void testEqualsOnTwoDifferentConstructionRoutes()
    {
        TimeWindowOuter window = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     Duration.ofSeconds( Long.MIN_VALUE ),
                                                     Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 ) );

        TimeWindow innerWindow = TimeWindow.newBuilder()
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
                                                                                                 .setSeconds( Long.MIN_VALUE ) )
                                           .setLatestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                               .setSeconds( Long.MAX_VALUE )
                                                                                               .setNanos( 999_999_999 ) )
                                           .build();


        TimeWindowOuter equalWindow = new TimeWindowOuter.Builder( innerWindow ).build();

        assertEquals( window, equalWindow );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testHashCode()
    {
        TimeWindowOuter first = TimeWindowOuter.of( SECOND_TIME,
                                                    FIFTH_TIME,
                                                    Duration.ofSeconds( Long.MIN_VALUE ),
                                                    Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 ) );
        TimeWindowOuter second = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     TimeWindowOuter.DURATION_MIN,
                                                     TimeWindowOuter.DURATION_MAX );

        TimeWindowOuter third = TimeWindowOuter.of( SECOND_TIME,
                                                    FIFTH_TIME,
                                                    THIRD_TIME,
                                                    FOURTH_TIME,
                                                    Duration.ZERO,
                                                    Duration.ofHours( 120 ) );
        TimeWindowOuter fourth = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     THIRD_TIME,
                                                     FOURTH_TIME,
                                                     Duration.ZERO,
                                                     Duration.ofHours( 120 ) );
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
        TimeWindowOuter window = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     Duration.ZERO );

        //EQUAL
        assertTrue( window.compareTo( TimeWindowOuter.of( SECOND_TIME,
                                                          FIFTH_TIME,
                                                          Duration.ZERO ) ) == 0 );
        assertTrue( TimeWindowOuter.of( SECOND_TIME,
                                        FIFTH_TIME,
                                        Duration.ZERO )
                                   .compareTo( window ) == 0 );
        //Transitive
        //x.compareTo(y) > 0
        assertTrue( window.compareTo( TimeWindowOuter.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                                          FIFTH_TIME,
                                                          Duration.ZERO ) ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( TimeWindowOuter.of( Instant.parse( "1984-01-01T00:00:00Z" ),
                                        FIFTH_TIME,
                                        Duration.ZERO )
                                   .compareTo( TimeWindowOuter.of( FIRST_TIME,
                                                                   FIFTH_TIME,
                                                                   Duration.ZERO ) ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( window.compareTo( TimeWindowOuter.of( FIRST_TIME,
                                                          FIFTH_TIME,
                                                          Duration.ZERO ) ) > 0 );
        //DIFFERENCES ON EARLIEST TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( THIRD_TIME,
                                                          FIFTH_TIME,
                                                          Duration.ZERO ) ) < 0 );
        //DIFFERENCES ON LATEST TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( SECOND_TIME,
                                                          Instant.parse( "2011-12-31T11:59:59Z" ),
                                                          Duration.ZERO ) ) < 0 );

        //DIFFERENCES ON EARLIEST VALID TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( SECOND_TIME,
                                                          FIFTH_TIME,
                                                          SECOND_TIME,
                                                          Instant.MAX,
                                                          Duration.ZERO,
                                                          Duration.ZERO ) ) < 0 );

        //DIFFERENCES ON LATEST VALID TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( SECOND_TIME,
                                                          FIFTH_TIME,
                                                          Instant.MIN,
                                                          Instant.parse( "2011-12-31T11:59:59Z" ),
                                                          Duration.ZERO,
                                                          Duration.ZERO ) ) > 0 );

        //DIFFERENCES ON EARLIEST LEAD TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( SECOND_TIME,
                                                          FIFTH_TIME,
                                                          Duration.ofHours( 1 ) ) ) < 0 );
        //DIFFERENCES ON LATEST LEAD TIME
        assertTrue( window.compareTo( TimeWindowOuter.of( SECOND_TIME,
                                                          FIFTH_TIME,
                                                          Duration.ZERO,
                                                          Duration.ofHours( 1 ) ) ) < 0 );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests {@link TimeWindowOuter#toString()} against other instances.
     */

    @Test
    public void testToString()
    {
        //Construct a window from 1985-01-01T00:00:00Z to 2010-12-31T11:59:59Z with lead times of 6-120h
        TimeWindowOuter window = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     SECOND_TIME,
                                                     FIFTH_TIME,
                                                     Duration.ZERO,
                                                     Duration.ZERO );
        //Equality of strings for equal objects
        assertTrue( "Unexpected inequality between the string representation of two time windows that are equal.",
                    window.toString()
                          .equals( TimeWindowOuter.of( SECOND_TIME,
                                                       FIFTH_TIME,
                                                       SECOND_TIME,
                                                       FIFTH_TIME,
                                                       Duration.ZERO,
                                                       Duration.ZERO )
                                                  .toString() ) );

        //Equality against a benchmark
        assertEquals( "TimeWindowOuter[earliestReferenceTime=1985-01-01T00:00:00Z,latestReferenceTime="
                      + "2010-12-31T11:59:59Z,earliestValidTime=1985-01-01T00:00:00Z,"
                      + "latestValidTime=2010-12-31T11:59:59Z,earliestLeadDuration=PT0S,latestLeadDuration=PT0S]",
                      window.toString() );
    }

    @Test
    public void testGetTimeWindow()
    {
        TimeWindowOuter window = TimeWindowOuter.of( SECOND_TIME,
                                                     FIFTH_TIME,
                                                     Duration.ZERO );

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
        IllegalArgumentException thrown = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeWindowOuter.of( SECOND_TIME,
                                                                                  Instant.parse( "1984-12-31T11:59:59Z" ),
                                                                                  Duration.ZERO ) );
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
        IllegalArgumentException thrown = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeWindowOuter.of( SECOND_TIME,
                                                                                  Instant.parse( "1986-12-31T11:59:59Z" ),
                                                                                  SECOND_TIME,
                                                                                  Instant.parse( "1984-12-31T11:59:59Z" ),
                                                                                  Duration.ZERO,
                                                                                  Duration.ZERO ) );
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
        IllegalArgumentException thrown = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeWindowOuter.of( SECOND_TIME,
                                                                                  FIFTH_TIME,
                                                                                  Duration.ofHours( 1 ),
                                                                                  Duration.ZERO ) );
        assertEquals( "Cannot define a time window whose latest lead duration is before its earliest "
                      + "lead duration.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the earliest reference time is null.
     */

    @Test
    public void testExceptionWhenEarliestReferenceTimeIsNull()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowOuter.of( null,
                                                                              Instant.MAX,
                                                                              Duration.ZERO ) );
        assertEquals( "The earliest reference time cannot be null.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the latest reference time is null.
     */

    @Test
    public void testExceptionWhenLatestReferenceTimeIsNull()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowOuter.of( Instant.MIN,
                                                                              null,
                                                                              Duration.ofHours( 1 ),
                                                                              Duration.ZERO ) );
        assertEquals( "The latest reference time cannot be null.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the earliest valid time is null.
     */

    @Test
    public void testExceptionWhenEarliestValidTimeIsNull()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowOuter.of( Instant.MIN,
                                                                              Instant.MAX,
                                                                              null,
                                                                              Instant.MAX,
                                                                              Duration.ZERO,
                                                                              Duration.ZERO ) );
        assertEquals( "The earliest valid time cannot be null.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the latest valid time is null.
     */

    @Test
    public void testExceptionWhenLatestValidTimeIsNull()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowOuter.of( Instant.MIN,
                                                                              Instant.MAX,
                                                                              Instant.MIN,
                                                                              null,
                                                                              Duration.ZERO,
                                                                              Duration.ZERO ) );
        assertEquals( "The latest valid time cannot be null.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the earliest lead duration is null.
     */

    @Test
    public void testExceptionWhenEarliestLeadDurationIsNull()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowOuter.of( Instant.MIN,
                                                                              Instant.MAX,
                                                                              Instant.MIN,
                                                                              Instant.MAX,
                                                                              null,
                                                                              Duration.ZERO ) );
        assertEquals( "The earliest lead duration cannot be null.",
                      thrown.getMessage() );
    }

    /**
     * Constructs a {@link TimeWindowOuter} and tests for an exception when the latest lead duration is null.
     */

    @Test
    public void testExceptionWhenLatestLeadDurationTimeIsNull()
    {
        NullPointerException thrown = assertThrows( NullPointerException.class,
                                                    () -> TimeWindowOuter.of( Instant.MIN,
                                                                              Instant.MAX,
                                                                              Instant.MIN,
                                                                              Instant.MAX,
                                                                              Duration.ZERO,
                                                                              null ) );
        assertEquals( "The latest lead duration cannot be null.",
                      thrown.getMessage() );
    }

    /**
     * Tests {@link TimeWindowOuter#hasUnboundedReferenceTimes()}.
     */

    @Test
    public void testHasUnboundedReferenceTimes()
    {
        TimeWindowOuter bounded = TimeWindowOuter.of( SECOND_TIME,
                                                      SIXTH_TIME,
                                                      Duration.ZERO );

        assertFalse( bounded.hasUnboundedReferenceTimes() );

        TimeWindowOuter unbounded = TimeWindowOuter.of();
        assertTrue( unbounded.hasUnboundedReferenceTimes() );

        TimeWindowOuter partlyLow = TimeWindowOuter.of( Instant.MIN,
                                                        SIXTH_TIME,
                                                        Duration.ZERO );

        assertTrue( partlyLow.hasUnboundedReferenceTimes() );

        TimeWindowOuter partlyHigh = TimeWindowOuter.of( SECOND_TIME,
                                                         Instant.MAX,
                                                         Duration.ZERO );

        assertTrue( partlyHigh.hasUnboundedReferenceTimes() );
    }

    /**
     * Tests {@link TimeWindowOuter#hasUnboundedValidTimes()}.
     */

    @Test
    public void testHasUnboundedValidTimes()
    {
        TimeWindowOuter bounded = TimeWindowOuter.of( FIRST_TIME,
                                                      SECOND_TIME,
                                                      THIRD_TIME,
                                                      FOURTH_TIME );

        assertFalse( bounded.hasUnboundedValidTimes() );

        TimeWindowOuter unbounded = TimeWindowOuter.of();

        assertTrue( unbounded.hasUnboundedValidTimes() );

        TimeWindowOuter partlyLow = TimeWindowOuter.of( FIRST_TIME,
                                                        SECOND_TIME,
                                                        Instant.MIN,
                                                        FOURTH_TIME );

        assertTrue( partlyLow.hasUnboundedValidTimes() );

        TimeWindowOuter partlyHigh = TimeWindowOuter.of( FIRST_TIME,
                                                         SECOND_TIME,
                                                         THIRD_TIME,
                                                         Instant.MAX );

        assertTrue( partlyHigh.hasUnboundedValidTimes() );
    }

    /**
     * Tests {@link TimeWindowOuter#bothLeadDurationsAreUnbounded()}.
     */

    @Test
    public void testBothLeadDurationsAreUnbounded()
    {
        TimeWindowOuter bounded = TimeWindowOuter.of( FIRST_TIME,
                                                      SECOND_TIME,
                                                      Duration.ZERO );

        assertFalse( bounded.bothLeadDurationsAreUnbounded() );

        TimeWindowOuter unbounded = TimeWindowOuter.of();

        assertTrue( unbounded.bothLeadDurationsAreUnbounded() );
    }

    /**
     * Tests the {@link TimeWindowOuter#unionOf(Set)}.
     */

    @Test
    public void testUnionOf()
    {
        TimeWindowOuter first = TimeWindowOuter.of( SECOND_TIME,
                                                    SEVENTH_TIME,
                                                    Duration.ofHours( 5 ),
                                                    Duration.ofHours( 25 ) );
        TimeWindowOuter second = TimeWindowOuter.of( FIRST_TIME,
                                                     SIXTH_TIME,
                                                     Duration.ofHours( -5 ),
                                                     Duration.ofHours( 20 ) );
        TimeWindowOuter expected = TimeWindowOuter.of( FIRST_TIME,
                                                       SEVENTH_TIME,
                                                       Duration.ofHours( -5 ),
                                                       Duration.ofHours( 25 ) );
        Set<TimeWindowOuter> union = new HashSet<>();
        union.add( first );
        union.add( second );

        TimeWindowOuter actual = TimeWindowOuter.unionOf( union );

        assertEquals( expected, actual );


        TimeWindowOuter third = TimeWindowOuter.of( SECOND_TIME,
                                                    SEVENTH_TIME,
                                                    FIRST_TIME,
                                                    Instant.parse( "2019-12-31T11:59:59Z" ),
                                                    Duration.ofHours( 5 ),
                                                    Duration.ofHours( 21 ) );
        TimeWindowOuter fourth = TimeWindowOuter.of( FIRST_TIME,
                                                     SIXTH_TIME,
                                                     Instant.parse( "1982-01-01T00:00:00Z" ),
                                                     SEVENTH_TIME,
                                                     Duration.ZERO,
                                                     Duration.ofHours( 20 ) );
        TimeWindowOuter expectedTwo = TimeWindowOuter.of( FIRST_TIME,
                                                          SEVENTH_TIME,
                                                          Instant.parse( "1982-01-01T00:00:00Z" ),
                                                          Instant.parse( "2019-12-31T11:59:59Z" ),
                                                          Duration.ZERO,
                                                          Duration.ofHours( 21 ) );
        Set<TimeWindowOuter> unionTwo = new HashSet<>();
        unionTwo.add( third );
        unionTwo.add( fourth );

        TimeWindowOuter actualTwo = TimeWindowOuter.unionOf( unionTwo );

        assertEquals( expectedTwo, actualTwo );
    }

    @Test
    public void testToBuilder()
    {
        TimeWindowOuter expected = TimeWindowOuter.of( SECOND_TIME,
                                                       FIFTH_TIME,
                                                       THIRD_TIME,
                                                       FOURTH_TIME,
                                                       Duration.ZERO,
                                                       Duration.ofHours( 120 ) );

        TimeWindowOuter actual = expected.toBuilder()
                                         .build();

        assertEquals( expected, actual );
    }

    /**
     * Tests that {@link TimeWindowOuter#unionWith(TimeWindowOuter)} throws an {@link IllegalArgumentException} on empty input.
     */

    @Test
    public void testUnionWithThrowsExceptionOnEmptyInput()
    {
        IllegalArgumentException thrown =
                assertThrows( IllegalArgumentException.class,
                              () -> TimeWindowOuter.unionOf( Set.of() ) );

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

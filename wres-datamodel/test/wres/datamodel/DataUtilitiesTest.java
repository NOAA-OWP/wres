package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdOuter.Builder;
import wres.datamodel.time.TimeWindowOuter;

/**
 * Tests the {@link DataUtilities}.
 * 
 * TODO: refactor the tests of containers (as opposed to factory methods) into their own test classes.
 * 
 * @author James Brown
 * @author jesse
 */
public final class DataUtilitiesTest
{
    /** First time for testing. */
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /** Second time for testing. */
    private static final String SECOND_TIME = "1986-01-01T00:00:00Z";

    /** Threshold label. */
    private static final String THRESHOLD_LABEL = "a threshold";

    /**
     * Tests for the correct implementation of {@link Comparable} by the {@link Pair}.
     */

    @Test
    public void compareDefaultMapBiKeyTest()
    {
        //Test equality
        Pair<TimeWindowOuter, ThresholdOuter> first =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        Pair<TimeWindowOuter, ThresholdOuter> second =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertTrue( first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        Pair<TimeWindowOuter, ThresholdOuter> third =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertTrue( third.compareTo( first ) > 0 );
        assertTrue( first.compareTo( third ) + third.compareTo( first ) == 0 );
        //Latest date
        Pair<TimeWindowOuter, ThresholdOuter> fourth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertTrue( third.compareTo( fourth ) > 0 );
        assertTrue( third.compareTo( fourth ) + fourth.compareTo( third ) == 0 );
        //Valid time
        Pair<TimeWindowOuter, ThresholdOuter> fifth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertTrue( fourth.compareTo( fifth ) < 0 );
        assertTrue( fourth.compareTo( fifth ) + fifth.compareTo( fourth ) == 0 );
        //Threshold
        Pair<TimeWindowOuter, ThresholdOuter> sixth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 0.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertTrue( fifth.compareTo( sixth ) > 0 );
        assertTrue( fifth.compareTo( sixth ) + sixth.compareTo( fifth ) == 0 );

        //Check nullity contract
        assertThrows( NullPointerException.class, () -> first.compareTo( null ) );
    }

    /**
     * Tests the {@link Pair#equals(Object)} and {@link Pair#hashCode()}.
     */

    @Test
    public void equalsHashCodePairTest()
    {
        //Equality
        Pair<TimeWindowOuter, ThresholdOuter> zeroeth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        Pair<TimeWindowOuter, ThresholdOuter> first =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        Pair<TimeWindowOuter, ThresholdOuter> second =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        //Reflexive
        assertEquals( first, first );

        //Symmetric 
        assertTrue( first.equals( second ) && second.equals( first ) );

        //Transitive 
        assertTrue( zeroeth.equals( first ) && first.equals( second ) && zeroeth.equals( second ) );

        //Nullity
        assertNotNull( first );

        //Check hashcode
        assertEquals( first.hashCode(), second.hashCode() );

        //Test inequalities
        //Earliest date
        Pair<TimeWindowOuter, ThresholdOuter> third =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertNotEquals( third, first );

        //Latest date
        Pair<TimeWindowOuter, ThresholdOuter> fourth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertNotEquals( third, fourth );

        //Valid time
        Pair<TimeWindowOuter, ThresholdOuter> fifth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertNotEquals( fourth, fifth );

        //Threshold
        Pair<TimeWindowOuter, ThresholdOuter> sixth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 0.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT ) );
        assertNotEquals( fifth, sixth );
    }

    /**
     * Constructs a {@link OneOrTwoThresholds} and tests {@link OneOrTwoThresholds#toStringSafe()} against other 
     * instances.
     */

    @Test
    public void testToStringSafeOneOrTwoThresholds()
    {
        OneOrTwoThresholds testString =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 27.0 ),
                                                                           OneOrTwoDoubles.of( 0.5 ),
                                                                           Operator.GREATER_EQUAL,
                                                                           ThresholdDataType.LEFT ) );

        assertTrue( "GTE_27.0_Pr_EQ_0.5".equals( DataUtilities.toStringSafe( testString ) ) );

        OneOrTwoThresholds secondTestString =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 23.0 ),
                                                                           OneOrTwoDoubles.of( 0.2 ),
                                                                           Operator.GREATER,
                                                                           ThresholdDataType.LEFT ),
                                       ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ) );

        assertTrue( "GT_23.0_Pr_EQ_0.2_AND_Pr_GT_0.1".equals( DataUtilities.toStringSafe( secondTestString ) ) );
    }

    /**
     * Tests the {@link ThresholdOuter#toStringSafe()}.
     */

    @Test
    public void testToStringSafeThresholdOuter()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .build();

        assertEquals( "GTE_0.0_Pr_EQ_0.0_AND_LT_0.5_Pr_EQ_0.7_a_threshold", DataUtilities.toStringSafe( threshold ) );

    }

    /**
     * See #79746
     */

    @Test
    public void testToStringSafeEliminatesReservedCharactersInUnits()
    {
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 23.0 ) )
                                                .setOperator( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setUnits( MeasurementUnit.of( "ft3/s" ) )
                                                .build();

        assertEquals( "GT_23.0_ft3s", DataUtilities.toStringSafe( threshold ) );
    }

    /**
     * Tests the {@link ThresholdOuter#toStringWithoutUnits()}.
     */

    @Test
    public void testToStringWithoutUnits()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( Operator.BETWEEN )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "CMS" ) )
                                                .build();

        assertEquals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)", threshold.toString() );

        assertEquals( ">= 0.0 [Pr = 0.0] AND < 0.5 [Pr = 0.7] (a threshold)",
                      DataUtilities.toStringWithoutUnits( threshold ) );
    }

    /**
     * Tests the {@link ThresholdOuter#toStringWithoutUnits()} with a unit string that contains regex characters. 
     * See #109152. These characters should be interpreted literally, not as a regex.
     */

    @Test
    public void testToStringWithoutUnitsForRegexString()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.5 ) )
                                                .setOperator( Operator.GREATER )
                                                .setDataType( ThresholdDataType.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "[ft_i]3/s" ) )
                                                .build();

        assertEquals( "> 0.5 (a threshold)", DataUtilities.toStringWithoutUnits( threshold ) );
    }

    @Test
    public void testToStringSafeInstant()
    {
        Instant instant = Instant.parse( "2027-12-23T00:00:01Z" );

        String expected = "20271223T000001Z";
        String actual = DataUtilities.toStringSafe( instant );

        assertEquals( expected, actual );

        String actualUpperBound = DataUtilities.toStringSafe( Instant.MAX );
        String expectedUpperBound = "MAXDATE";

        assertEquals( expectedUpperBound, actualUpperBound );
    }
}

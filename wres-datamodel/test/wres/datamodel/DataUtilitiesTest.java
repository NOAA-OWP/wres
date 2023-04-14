package wres.datamodel;

import static org.apache.commons.math3.util.Precision.EPSILON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdOuter.Builder;
import wres.datamodel.time.TimeWindowOuter;

/**
 * <p>Tests the {@link DataUtilities}.
 * 
 * <p>TODO: refactor the tests of containers (as opposed to factory methods) into their own test classes.
 * 
 * @author James Brown
 * @author jesse
 */
final class DataUtilitiesTest
{
    /** First time for testing. */
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /** Second time for testing. */
    private static final String SECOND_TIME = "1986-01-01T00:00:00Z";

    /** Threshold label. */
    private static final String THRESHOLD_LABEL = "a threshold";

    @Test
    void testParsePointFromPointWkt()
    {
        String wkt = "POINT ( 3.141592654 5.1 )";
        Coordinate point = DataUtilities.getLonLatFromPointWkt( wkt );
        assertEquals( 3.141592654, point.getX(), EPSILON );
        assertEquals( 5.1, point.getY(), EPSILON );
    }

    @Test
    void testParsePointFromPointWktNonStrict()
    {
        String wkt = "POINT ( 3.141592654 5.1 )";
        Coordinate point = DataUtilities.getLonLatOrNullFromWkt( wkt );
        assertNotNull( point );
        assertEquals( 3.141592654, point.getX(), EPSILON );
        assertEquals( 5.1, point.getY(), EPSILON );
    }

    @Test
    void testParsePointFromPointWktNonStrictExpectsNull()
    {
        String wkt = "foo";
        Coordinate point = DataUtilities.getLonLatOrNullFromWkt( wkt );
        assertNull( point );
    }

    @Test
    void compareDefaultMapBiKeyTest()
    {
        //Test equality
        Pair<TimeWindowOuter, ThresholdOuter> first =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        Pair<TimeWindowOuter, ThresholdOuter> second =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertTrue( first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        Pair<TimeWindowOuter, ThresholdOuter> third =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertTrue( third.compareTo( first ) > 0 );
        assertEquals( 0, first.compareTo( third ) + third.compareTo( first ) );
        //Latest date
        Pair<TimeWindowOuter, ThresholdOuter> fourth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertTrue( third.compareTo( fourth ) > 0 );
        assertEquals( 0, third.compareTo( fourth ) + fourth.compareTo( third ) );
        //Valid time
        Pair<TimeWindowOuter, ThresholdOuter> fifth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertTrue( fourth.compareTo( fifth ) < 0 );
        assertEquals( 0, fourth.compareTo( fifth ) + fifth.compareTo( fourth ) );
        //Threshold
        Pair<TimeWindowOuter, ThresholdOuter> sixth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 0.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertTrue( fifth.compareTo( sixth ) > 0 );
        assertEquals( 0, fifth.compareTo( sixth ) + sixth.compareTo( fifth ) );

        //Check nullity contract
        assertThrows( NullPointerException.class, () -> first.compareTo( null ) );
    }

    @Test
    void equalsHashCodePairTest()
    {
        //Equality
        Pair<TimeWindowOuter, ThresholdOuter> zeroeth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        Pair<TimeWindowOuter, ThresholdOuter> first =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        Pair<TimeWindowOuter, ThresholdOuter> second =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                           Instant.MAX ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
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
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertNotEquals( third, first );

        //Latest date
        Pair<TimeWindowOuter, ThresholdOuter> fourth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertNotEquals( third, fourth );

        //Valid time
        Pair<TimeWindowOuter, ThresholdOuter> fifth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertNotEquals( fourth, fifth );

        //Threshold
        Pair<TimeWindowOuter, ThresholdOuter> sixth =
                Pair.of( TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ),
                                                                           Instant.parse( FIRST_TIME ),
                                                                           Instant.parse( SECOND_TIME ) ) ),
                         ThresholdOuter.of( OneOrTwoDoubles.of( 0.0 ),
                                            ThresholdOperator.GREATER,
                                            ThresholdOrientation.LEFT ) );
        assertNotEquals( fifth, sixth );
    }

    @Test
    void testToStringSafeOneOrTwoThresholds()
    {
        OneOrTwoThresholds testString =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 27.0 ),
                                                                           OneOrTwoDoubles.of( 0.5 ),
                                                                           ThresholdOperator.GREATER_EQUAL,
                                                                           ThresholdOrientation.LEFT ) );

        assertEquals( "GTE_27.0_Pr_EQ_0.5", DataUtilities.toStringSafe( testString ) );

        OneOrTwoThresholds secondTestString =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 23.0 ),
                                                                           OneOrTwoDoubles.of( 0.2 ),
                                                                           ThresholdOperator.GREATER,
                                                                           ThresholdOrientation.LEFT ),
                                       ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                              ThresholdOperator.GREATER,
                                                                              ThresholdOrientation.LEFT ) );

        assertEquals( "GT_23.0_Pr_EQ_0.2_AND_Pr_GT_0.1", DataUtilities.toStringSafe( secondTestString ) );
    }

    @Test
    void testToStringSafeThresholdOuter()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( ThresholdOperator.BETWEEN )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .build();

        assertEquals( "GTE_0.0_Pr_EQ_0.0_AND_LT_0.5_Pr_EQ_0.7_a_threshold", DataUtilities.toStringSafe( threshold ) );

    }

    /**
     * See #79746
     */

    @Test
    void testToStringSafeEliminatesReservedCharactersInUnits()
    {
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 23.0 ) )
                                                .setOperator( ThresholdOperator.GREATER )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .setUnits( MeasurementUnit.of( "ft3/s" ) )
                                                .build();

        assertEquals( "GT_23.0_ft3s", DataUtilities.toStringSafe( threshold ) );
    }

    @Test
    void testToStringWithoutUnits()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.0, 0.5 ) )
                                                .setProbabilities( OneOrTwoDoubles.of( 0.0, 0.7 ) )
                                                .setOperator( ThresholdOperator.BETWEEN )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "CMS" ) )
                                                .build();

        assertEquals( ">= 0.0 CMS [Pr = 0.0] AND < 0.5 CMS [Pr = 0.7] (a threshold)", threshold.toString() );

        assertEquals( ">= 0.0 [Pr = 0.0] AND < 0.5 [Pr = 0.7] (a threshold)",
                      DataUtilities.toStringWithoutUnits( threshold ) );
    }

    /**
     * Tests the {@link DataUtilities#toStringWithoutUnits(ThresholdOuter)} with a unit string that contains regex
     * characters. See #109152. These characters should be interpreted literally, not as a regex.
     */

    @Test
    void testToStringWithoutUnitsForRegexString()
    {
        // All components
        ThresholdOuter threshold = new Builder().setValues( OneOrTwoDoubles.of( 0.5 ) )
                                                .setOperator( ThresholdOperator.GREATER )
                                                .setDataType( ThresholdOrientation.LEFT )
                                                .setLabel( THRESHOLD_LABEL )
                                                .setUnits( MeasurementUnit.of( "[ft_i]3/s" ) )
                                                .build();

        assertEquals( "> 0.5 (a threshold)", DataUtilities.toStringWithoutUnits( threshold ) );
    }

    @Test
    void testToStringSafeInstant()
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

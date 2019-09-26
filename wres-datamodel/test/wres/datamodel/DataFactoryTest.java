package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.MetricConfigName;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;

/**
 * Tests the {@link DataFactory}.
 * 
 * TODO: refactor the tests of containers (as opposed to factory methods) into their own test classes.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 */
public final class DataFactoryTest
{

    /**
     * Expected exception on null input.
     */

    private static final String EXPECTED_EXCEPTION_ON_NULL =
            "Specify input configuration with a non-null identifier to map.";

    /**
     * Location for metadata.
     */

    private static final String DRRC2 = "DRRC2";

    /**
     * Second time for testing.
     */

    private static final String SECOND_TIME = "1986-01-01T00:00:00Z";

    /**
     * First time for testing.
     */

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    public static final double THRESHOLD = 0.00001;

    @Test
    public void vectorOfDoublesTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = VectorOfDoubles.of( arrOne );
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], THRESHOLD );
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = VectorOfDoubles.of( arrOne );
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], THRESHOLD );
    }

    @Test
    public void pairOfVectorsTest()
    {
        final double[] arrOne = { 1.0, 2.0, 3.0 };
        final double[] arrTwo = { 4.0, 5.0 };
        final Pair<VectorOfDoubles, VectorOfDoubles> pair = DataFactory.pairOf( arrOne, arrTwo );
        assertNotNull( pair );
        assertEquals( 1.0, pair.getLeft().getDoubles()[0], THRESHOLD );
        assertEquals( 2.0, pair.getLeft().getDoubles()[1], THRESHOLD );
        assertEquals( 3.0, pair.getLeft().getDoubles()[2], THRESHOLD );
        assertEquals( 4.0, pair.getRight().getDoubles()[0], THRESHOLD );
        assertEquals( 5.0, pair.getRight().getDoubles()[1], THRESHOLD );
    }

    /**
     * Tests for the correct implementation of {@link Comparable} by the {@link Pair}.
     */

    @Test
    public void compareDefaultMapBiKeyTest()
    {
        //Test equality
        Pair<TimeWindow, Threshold> first = Pair.of( TimeWindow.of( Instant.MIN,
                                                                    Instant.MAX ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> second = Pair.of( TimeWindow.of( Instant.MIN,
                                                                     Instant.MAX ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );
        assertTrue( first.compareTo( second ) == 0 && second.compareTo( first ) == 0 && first.equals( second ) );
        //Test inequality and anticommutativity 
        //Earliest date
        Pair<TimeWindow, Threshold> third = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                    Instant.MAX ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        assertTrue( third.compareTo( first ) > 0 );
        assertTrue( first.compareTo( third ) + third.compareTo( first ) == 0 );
        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                     Instant.parse( SECOND_TIME ) ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );
        assertTrue( third.compareTo( fourth ) > 0 );
        assertTrue( third.compareTo( fourth ) + fourth.compareTo( third ) == 0 );
        //Valid time
        Pair<TimeWindow, Threshold> fifth = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ),
                                                                    Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ) ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        assertTrue( fourth.compareTo( fifth ) < 0 );
        assertTrue( fourth.compareTo( fifth ) + fifth.compareTo( fourth ) == 0 );
        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ),
                                                                    Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ) ),
                                                     Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        assertTrue( fifth.compareTo( sixth ) > 0 );
        assertTrue( fifth.compareTo( sixth ) + sixth.compareTo( fifth ) == 0 );

        //Check nullity contract
        exception.expect( NullPointerException.class );
        first.compareTo( null );

    }

    /**
     * Tests the {@link Pair#equals(Object)} and {@link Pair#hashCode()}.
     */

    @Test
    public void equalsHashCodePairTest()
    {
        //Equality
        Pair<TimeWindow, Threshold> zeroeth = Pair.of( TimeWindow.of( Instant.MIN,
                                                                      Instant.MAX ),
                                                       Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> first = Pair.of( TimeWindow.of( Instant.MIN,
                                                                    Instant.MAX ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        Pair<TimeWindow, Threshold> second = Pair.of( TimeWindow.of( Instant.MIN,
                                                                     Instant.MAX ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
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
        Pair<TimeWindow, Threshold> third = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                    Instant.MAX ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        assertNotEquals( third, first );

        //Latest date
        Pair<TimeWindow, Threshold> fourth = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                     Instant.parse( SECOND_TIME ) ),
                                                      Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );
        assertNotEquals( third, fourth );

        //Valid time
        Pair<TimeWindow, Threshold> fifth = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ),
                                                                    Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ) ),
                                                     Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        assertNotEquals( fourth, fifth );

        //Threshold
        Pair<TimeWindow, Threshold> sixth = Pair.of( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ),
                                                                    Instant.parse( FIRST_TIME ),
                                                                    Instant.parse( SECOND_TIME ) ),
                                                     Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT ) );
        assertNotEquals( fifth, sixth );
    }

    /**
     * Tests the {@link ProjectConfigs#getThresholdOperator(ThresholdsConfig)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testGetThresholdOperator()
    {
        ThresholdsConfig first = new ThresholdsConfig( null,
                                                       null,
                                                       null,
                                                       ThresholdOperator.GREATER_THAN );
        assertTrue( DataFactory.getThresholdOperator( first ) == Operator.GREATER );

        ThresholdsConfig second = new ThresholdsConfig( null,
                                                        null,
                                                        null,
                                                        ThresholdOperator.LESS_THAN );
        assertTrue( DataFactory.getThresholdOperator( second ) == Operator.LESS );

        ThresholdsConfig third = new ThresholdsConfig( null,
                                                       null,
                                                       null,
                                                       ThresholdOperator.GREATER_THAN_OR_EQUAL_TO );
        assertTrue( DataFactory.getThresholdOperator( third ) == Operator.GREATER_EQUAL );

        ThresholdsConfig fourth = new ThresholdsConfig( null,
                                                        null,
                                                        null,
                                                        ThresholdOperator.LESS_THAN_OR_EQUAL_TO );
        assertTrue( DataFactory.getThresholdOperator( fourth ) == Operator.LESS_EQUAL );

        //Test exception cases
        exception.expect( NullPointerException.class );

        DataFactory.getThresholdOperator( (ThresholdsConfig) null );

        DataFactory.getThresholdOperator( new ThresholdsConfig( null,
                                                                null,
                                                                null,
                                                                null ) );
    }

    /**
     * Tests the {@link DataFactory#getMetricName(wres.config.generated.MetricConfigName)}.
     */

    @Test
    public void testGetMetricName()
    {
        // The MetricConfigName.ALL_VALID returns null        
        assertNull( DataFactory.getMetricName( MetricConfigName.ALL_VALID ) );

        // Check that a mapping exists in MetricConstants for all entries in the MetricConfigName
        for ( MetricConfigName next : MetricConfigName.values() )
        {
            if ( next != MetricConfigName.ALL_VALID )
            {
                assertNotNull( DataFactory.getMetricName( next ) );
            }
        }
    }

    /**
     * Tests the {@link DataFactory#getMetricName(wres.config.generated.MetricConfigName)} throws an 
     * expected exception when the input is null.
     */

    @Test
    public void testGetMetricNameThrowsNPEWhenInputIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( EXPECTED_EXCEPTION_ON_NULL );

        DataFactory.getMetricName( (MetricConfigName) null );
    }

    /**
     * Tests the {@link DataFactory#getMetricName(wres.config.generated.TimeSeriesMetricConfigName)}.
     */

    @Test
    public void testGetTimeSeriesMetricName()
    {
        // The TimeSeriesMetricConfigName.ALL_VALID returns null        
        assertNull( DataFactory.getMetricName( TimeSeriesMetricConfigName.ALL_VALID ) );

        // Check that a mapping exists in MetricConstants for all entries in the TimeSeriesMetricConfigName
        for ( TimeSeriesMetricConfigName next : TimeSeriesMetricConfigName.values() )
        {
            if ( next != TimeSeriesMetricConfigName.ALL_VALID )
            {
                assertNotNull( DataFactory.getMetricName( next ) );
            }
        }
    }

    /**
     * Tests the {@link DataFactory#getMetricName(wres.config.generated.TimeSeriesMetricConfigName)} throws an 
     * expected exception when the input is null.
     */

    @Test
    public void testGetTimeSeriesMetricNameThrowsNPEWhenInputIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( EXPECTED_EXCEPTION_ON_NULL );

        DataFactory.getMetricName( (TimeSeriesMetricConfigName) null );
    }

    /**
     * Tests the {@link DataFactory#getThresholdDataType(wres.config.generated.ThresholdDataType)}.
     */

    @Test
    public void testGetThresholdDataType()
    {
        // Check that a mapping exists in the data model ThresholdDataType for all entries in the 
        // config ThresholdDataType
        for ( wres.config.generated.ThresholdDataType next : wres.config.generated.ThresholdDataType.values() )
        {
            assertNotNull( DataFactory.getThresholdDataType( next ) );
        }
    }

    /**
     * Tests the {@link DataFactory#getThresholdDataType(wres.config.generated.ThresholdDataType)} throws an 
     * expected exception when the input is null.
     */

    @Test
    public void testGetThresholdDataTypeThrowsNPEWhenInputIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( EXPECTED_EXCEPTION_ON_NULL );

        DataFactory.getThresholdDataType( null );
    }

    /**
     * Tests the {@link DataFactory#getThresholdGroup(wres.config.generated.ThresholdType)}.
     */

    @Test
    public void testGetThresholdGroup()
    {
        // Check that a mapping exists in ThresholdGroup for all entries in the ThresholdType
        for ( ThresholdType next : ThresholdType.values() )
        {
            assertNotNull( DataFactory.getThresholdGroup( next ) );
        }
    }

    /**
     * Tests the {@link DataFactory#getThresholdDataType(wres.config.generated.ThresholdDataType)} throws an 
     * expected exception when the input is null.
     */

    @Test
    public void testGetThresholdGroupThrowsNPEWhenInputIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( EXPECTED_EXCEPTION_ON_NULL );

        DataFactory.getThresholdGroup( null );
    }

}

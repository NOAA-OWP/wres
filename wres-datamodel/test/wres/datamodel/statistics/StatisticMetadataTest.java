package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;

/**
 * Tests the {@link StatisticMetadata}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticMetadataTest
{

    private static final String OTHER_TEST_DIMENSION = "SOME_DIM";
    private static final String TEST_DIMENSION = "OTHER_DIM";
    private static final String DRRC3 = "DRRC3";

    /**
     * Test {@link StatisticMetadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1986-01-01T00:00:00Z" ) );
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        Location locationBase = Location.of( DRRC3 );
        SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( OTHER_TEST_DIMENSION ),
                                                 DatasetIdentifier.of( locationBase,
                                                                       "SQIN",
                                                                       "HEFS" ),
                                                 firstWindow,
                                                 thresholds );

        StatisticMetadata first = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        StatisticMetadata second = StatisticMetadata.of( base,
                                                         1,
                                                         MeasurementUnit.of( "CMS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        // Reflexive
        assertTrue( first.equals( first ) );
        // Symmetric
        assertTrue( first.equals( second ) );
        assertTrue( second.equals( first ) );
        // Transitive
        StatisticMetadata secondT = StatisticMetadata.of( base,
                                                          1,
                                                          MeasurementUnit.of( "CMS" ),
                                                          MetricConstants.BIAS_FRACTION,
                                                          null );
        assertTrue( second.equals( secondT ) );
        assertTrue( first.equals( secondT ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( first.equals( second ) );
        }

        // Unequal
        StatisticMetadata third = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        assertFalse( first.equals( third ) );
        StatisticMetadata fourth = StatisticMetadata.of( base,
                                                         1,
                                                         MeasurementUnit.of( "CFS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        assertFalse( first.equals( fourth ) );
        StatisticMetadata fifth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        null );
        assertFalse( first.equals( fifth ) );
        StatisticMetadata sixth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        MetricConstants.NONE );
        assertFalse( first.equals( sixth ) );
        // Unequal input dimensions
        Location seventhLocation = Location.of( DRRC3 );
        final TimeWindow timeWindow = firstWindow;
        StatisticMetadata seventh =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                                 .setIdentifier( DatasetIdentifier.of( seventhLocation,
                                                                                                       "SQIN",
                                                                                                       "HEFS" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      2,
                                      MeasurementUnit.of( "CMS" ),
                                      MetricConstants.BIAS_FRACTION,
                                      null );
        assertFalse( third.equals( seventh ) );
        // Null check
        assertNotEquals( null, first );
        // Other type check
        assertNotEquals( Double.valueOf( 2 ) , first );
    }

    /**
     * Test {@link StatisticMetadata#minimumEquals(StatisticMetadata)}.
     */

    @Test
    public void testMinimumEquals()
    {
        Location locationBase = Location.of( DRRC3 );
        SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( OTHER_TEST_DIMENSION ),
                                                 DatasetIdentifier.of( locationBase,
                                                                       "SQIN",
                                                                       "HEFS" ) );
        StatisticMetadata first = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        StatisticMetadata second = StatisticMetadata.of( base,
                                                         2,
                                                         MeasurementUnit.of( "CMS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        // Not equal according to stricter equals
        assertFalse( first.equals( second ) );
        // Reflexive
        assertTrue( first.minimumEquals( first ) );
        // Symmetric
        assertTrue( first.minimumEquals( second ) );
        assertTrue( second.minimumEquals( first ) );
        // Transitive
        StatisticMetadata secondT = StatisticMetadata.of( base,
                                                          1,
                                                          MeasurementUnit.of( "CMS" ),
                                                          MetricConstants.BIAS_FRACTION,
                                                          null );

        assertTrue( second.minimumEquals( secondT ) );
        assertTrue( first.minimumEquals( secondT ) );
        // Unequal
        StatisticMetadata third = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        null );
        assertFalse( first.minimumEquals( third ) );
        StatisticMetadata fourth = StatisticMetadata.of( base,
                                                         2,
                                                         MeasurementUnit.of( "CMS" ),
                                                         MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                         MetricConstants.NONE );
        assertFalse( third.minimumEquals( fourth ) );
        StatisticMetadata fifth = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CFS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        MetricConstants.NONE );
        assertFalse( fourth.minimumEquals( fifth ) );
        Location secondLocation = Location.of( DRRC3 );
        SampleMetadata baseSecond = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                       DatasetIdentifier.of( secondLocation,
                                                                             "SQIN",
                                                                             "HEFS" ) );

        StatisticMetadata sixth = StatisticMetadata.of( baseSecond,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        assertFalse( first.minimumEquals( sixth ) );

    }

    /**
     * Test {@link StatisticMetadata#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1986-01-01T00:00:00Z" ) );

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        Location baseLocation = Location.of( DRRC3 );
        SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( OTHER_TEST_DIMENSION ),
                                                 DatasetIdentifier.of( baseLocation,
                                                                       "SQIN",
                                                                       "HEFS" ),
                                                 firstWindow,
                                                 thresholds );
        StatisticMetadata first = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        StatisticMetadata second = StatisticMetadata.of( base,
                                                         1,
                                                         MeasurementUnit.of( "CMS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        assertEquals( first.hashCode(), first.hashCode() );
        assertEquals( first.hashCode(), second.hashCode() );
        StatisticMetadata secondT = StatisticMetadata.of( base,
                                                          1,
                                                          MeasurementUnit.of( "CMS" ),
                                                          MetricConstants.BIAS_FRACTION,
                                                          null );
        assertTrue( second.hashCode() == secondT.hashCode() );
        assertTrue( first.hashCode() == secondT.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata hashcodes.",
                        first.hashCode() == second.hashCode() );
        }
        // Unequal
        StatisticMetadata third = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        assertFalse( first.hashCode() == third.hashCode() );
        StatisticMetadata fourth = StatisticMetadata.of( base,
                                                         1,
                                                         MeasurementUnit.of( "CFS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        assertFalse( first.hashCode() == fourth.hashCode() );
        StatisticMetadata fifth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        null );
        assertFalse( first.hashCode() == fifth.hashCode() );
        StatisticMetadata sixth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        MetricConstants.NONE );
        assertFalse( first.hashCode() == sixth.hashCode() );
        // Unequal input dimensions
        Location seventhLocation = Location.of( DRRC3 );
        final TimeWindow timeWindow = firstWindow;
        StatisticMetadata seventh =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                                 .setIdentifier( DatasetIdentifier.of( seventhLocation,
                                                                                                       "SQIN",
                                                                                                       "HEFS" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      2,
                                      MeasurementUnit.of( "CMS" ),
                                      MetricConstants.BIAS_FRACTION,
                                      null );
        assertFalse( third.hashCode() == seventh.hashCode() );
        // Other type check
        assertFalse( first.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}

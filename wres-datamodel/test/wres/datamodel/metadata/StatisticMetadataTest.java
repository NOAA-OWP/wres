package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link StatisticMetadata}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticMetadataTest
{

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

        Location locationBase = Location.of( "DRRC3" );
        SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( "SOME_DIM" ),
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
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( first ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( second ) );
        assertTrue( "Unexpected inequality between two metadata instances.", second.equals( first ) );
        // Transitive
        StatisticMetadata secondT = StatisticMetadata.of( base,
                                                          1,
                                                          MeasurementUnit.of( "CMS" ),
                                                          MetricConstants.BIAS_FRACTION,
                                                          null );
        assertTrue( "Unexpected inequality between two metadata instances.", second.equals( secondT ) );
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( secondT ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata instances.", first.equals( second ) );
        }

        // Unequal
        StatisticMetadata third = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( third ) );
        StatisticMetadata fourth = StatisticMetadata.of( base,
                                                         1,
                                                         MeasurementUnit.of( "CFS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( fourth ) );
        StatisticMetadata fifth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( fifth ) );
        StatisticMetadata sixth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( sixth ) );
        // Unequal input dimensions
        Location seventhLocation = Location.of( "DRRC3" );
        final TimeWindow timeWindow = firstWindow;
        StatisticMetadata seventh =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "OTHER_DIM" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( seventhLocation,
                                                                                                       "SQIN",
                                                                                                       "HEFS" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      2,
                                      MeasurementUnit.of( "CMS" ),
                                      MetricConstants.BIAS_FRACTION,
                                      null );
        assertFalse( "Unexpected equality between two metadata instances.", third.equals( seventh ) );
        // Null check
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( null ) );
        // Other type check
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link StatisticMetadata#minimumEquals(StatisticMetadata)}.
     */

    @Test
    public void testMinimumEquals()
    {
        Location locationBase = Location.of( "DRRC3" );
        SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( "SOME_DIM" ),
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
        assertFalse( "Unexpected inequality between two metadata instances.", first.equals( second ) );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", first.minimumEquals( first ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", first.minimumEquals( second ) );
        assertTrue( "Unexpected inequality between two metadata instances.", second.minimumEquals( first ) );
        // Transitive
        StatisticMetadata secondT = StatisticMetadata.of( base,
                                                          1,
                                                          MeasurementUnit.of( "CMS" ),
                                                          MetricConstants.BIAS_FRACTION,
                                                          null );

        assertTrue( "Unexpected inequality between two metadata instances.", second.minimumEquals( secondT ) );
        assertTrue( "Unexpected inequality between two metadata instances.", first.minimumEquals( secondT ) );
        // Unequal
        StatisticMetadata third = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.minimumEquals( third ) );
        StatisticMetadata fourth = StatisticMetadata.of( base,
                                                         2,
                                                         MeasurementUnit.of( "CMS" ),
                                                         MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                         MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", third.minimumEquals( fourth ) );
        StatisticMetadata fifth = StatisticMetadata.of( base,
                                                        2,
                                                        MeasurementUnit.of( "CFS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", fourth.minimumEquals( fifth ) );
        Location secondLocation = Location.of( "DRRC3" );
        SampleMetadata baseSecond = SampleMetadata.of( MeasurementUnit.of( "OTHER_DIM" ),
                                                       DatasetIdentifier.of( secondLocation,
                                                                             "SQIN",
                                                                             "HEFS" ) );

        StatisticMetadata sixth = StatisticMetadata.of( baseSecond,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.minimumEquals( sixth ) );

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

        Location baseLocation = Location.of( "DRRC3" );
        SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( "SOME_DIM" ),
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
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == first.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == second.hashCode() );
        StatisticMetadata secondT = StatisticMetadata.of( base,
                                                          1,
                                                          MeasurementUnit.of( "CMS" ),
                                                          MetricConstants.BIAS_FRACTION,
                                                          null );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", second.hashCode() == secondT.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == secondT.hashCode() );
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
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == third.hashCode() );
        StatisticMetadata fourth = StatisticMetadata.of( base,
                                                         1,
                                                         MeasurementUnit.of( "CFS" ),
                                                         MetricConstants.BIAS_FRACTION,
                                                         null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == fourth.hashCode() );
        StatisticMetadata fifth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                        null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == fifth.hashCode() );
        StatisticMetadata sixth = StatisticMetadata.of( base,
                                                        1,
                                                        MeasurementUnit.of( "CMS" ),
                                                        MetricConstants.BIAS_FRACTION,
                                                        MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == sixth.hashCode() );
        // Unequal input dimensions
        Location seventhLocation = Location.of( "DRRC3" );
        final TimeWindow timeWindow = firstWindow;
        StatisticMetadata seventh =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "OTHER_DIM" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( seventhLocation,
                                                                                                       "SQIN",
                                                                                                       "HEFS" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      2,
                                      MeasurementUnit.of( "CMS" ),
                                      MetricConstants.BIAS_FRACTION,
                                      null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", third.hashCode() == seventh.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two metadata hashcodes.",
                     first.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}

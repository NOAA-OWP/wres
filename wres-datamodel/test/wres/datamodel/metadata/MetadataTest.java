package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetadataTest
{

    /**
     * Test {@link Metadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertTrue( "Unexpected inequality between two metadata instances.",
                    Metadata.of().equals( Metadata.of() ) );
        Location l1 = Location.of( "DRRC2" );
        Metadata m1 = Metadata.of( MeasurementUnit.of(),
                                   DatasetIdentifier.of( l1, "SQIN", "HEFS" ) );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m1 ) );
        Location l2 = Location.of( "DRRC2" );
        Metadata m2 = Metadata.of( MeasurementUnit.of(),
                                   DatasetIdentifier.of( l2, "SQIN", "HEFS" ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m2.equals( m1 ) );
        Location l3 = Location.of( "DRRC2" );
        Metadata m3 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l3, "SQIN", "HEFS" ) );
        Location l4 = Location.of( "DRRC2" );
        Metadata m4 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l4, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m1.equals( m3 ) );
        // Transitive
        Location l4t = Location.of( "DRRC2" );
        Metadata m4t = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                    DatasetIdentifier.of( l4t, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.equals( m4t ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4t ) );
        // Unequal
        Location l5 = Location.of( "DRRC3" );
        Metadata m5 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l5, "SQIN", "HEFS" ) );
        assertFalse( "Unexpected equality between two metadata instances.", m4.equals( m5 ) );
        Metadata m5NoDim = Metadata.of( MeasurementUnit.of( "SOME_DIM" ), null );
        assertFalse( "Unexpected equality between two metadata instances.", m5.equals( m5NoDim ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        }
        // Add a time window
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1986-01-01T00:00:00Z" ),
                                                ReferenceTime.VALID_TIME );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                 ReferenceTime.VALID_TIME );
        Location l6 = Location.of( "DRRC3" );
        Metadata m6 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l6, "SQIN", "HEFS" ),
                                   firstWindow );
        Location l7 = Location.of( "DRRC3" );
        Metadata m7 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l7, "SQIN", "HEFS" ),
                                   secondWindow );
        assertTrue( "Unexpected inequality between two metadata instances.", m6.equals( m7 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m7.equals( m6 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m3.equals( m6 ) );
        
        TimeWindow thirdWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1986-01-01T00:00:00Z" ),
                                                ReferenceTime.ISSUE_TIME );
        Location l8 = Location.of( "DRRC3" );
        Metadata m8 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l8, "SQIN", "HEFS" ),
                                   thirdWindow );
        assertFalse( "Unexpected equality between two metadata instances.", m6.equals( m8 ) );
        
        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        
        Metadata m9 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l8, "SQIN", "HEFS" ),
                                   thirdWindow,
                                   thresholds );
        
        assertFalse( "Unexpected equality between two metadata instances.", m8.equals( m9 ) );
        
        Metadata m10 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l8, "SQIN", "HEFS" ),
                                   thirdWindow,
                                   thresholds );
        
        assertTrue( "Unexpected inequality between two metadata instances.", m9.equals( m10 ) );
        
        // Null check
        assertFalse( "Unexpected equality between two metadata instances.", m6.equals( null ) );
        // Other type check
        assertFalse( "Unexpected equality between two metadata instances.", m6.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link Metadata#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertTrue( "Unexpected inequality between two metadata hashcodes.",
                    Metadata.of().hashCode() == Metadata.of().hashCode() );
        Location l1 = Location.of( "DRRC2" );
        Metadata m1 = Metadata.of( MeasurementUnit.of(),
                                   DatasetIdentifier.of( l1, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m1.hashCode() == m1.hashCode() );
        Location l2 = Location.of( "DRRC2" );
        Metadata m2 = Metadata.of( MeasurementUnit.of(),
                                   DatasetIdentifier.of( l2, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        Location l3 = Location.of( "DRRC2" );
        Metadata m3 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l3, "SQIN", "HEFS" ) );
        Location l4 = Location.of( "DRRC2" );
        Metadata m4 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l4, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m3.hashCode() == m4.hashCode() );
        Location l4t = Location.of( "DRRC2" );
        Metadata m4t = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                    DatasetIdentifier.of( l4t, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.hashCode() == m4t.hashCode() );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.hashCode() == m4t.hashCode() );
        // Unequal
        assertFalse( "Unexpected equality between two metadata hashcodes.", m1.hashCode() == m3.hashCode() );
        Location l5 = Location.of( "DRRC3" );
        Metadata m5 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l5, "SQIN", "HEFS" ) );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m4.hashCode() == m5.hashCode() );
        Metadata m5NoDim = Metadata.of( MeasurementUnit.of( "SOME_DIM" ), null );
        assertFalse( "Unexpected equality between two metadata instances.", m5.hashCode() == m5NoDim.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        }

        // Add a time window
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1986-01-01T00:00:00Z" ),
                                                ReferenceTime.VALID_TIME );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1986-01-01T00:00:00Z" ),
                                                 ReferenceTime.VALID_TIME );
        Location l6 = Location.of( "DRRC3" );
        Metadata m6 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l6, "SQIN", "HEFS" ),
                                   firstWindow );
        Location l7 = Location.of( "DRRC3" );
        Metadata m7 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l7, "SQIN", "HEFS" ),
                                   secondWindow );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m6.hashCode() == m7.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m7.hashCode() == m6.hashCode() );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m3.hashCode() == m6.hashCode() );
        TimeWindow thirdWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1986-01-01T00:00:00Z" ),
                                                ReferenceTime.ISSUE_TIME );
        Location l8 = Location.of( "DRRC3" );
        Metadata m8 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l8, "SQIN", "HEFS" ),
                                   thirdWindow );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m6.hashCode() == m8.hashCode() );
        
        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        
        Metadata m9 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l8, "SQIN", "HEFS" ),
                                   thirdWindow,
                                   thresholds );

        Metadata m10 = Metadata.of( MeasurementUnit.of( "SOME_DIM" ),
                                   DatasetIdentifier.of( l8, "SQIN", "HEFS" ),
                                   thirdWindow,
                                   thresholds );
        
        assertTrue( "Unexpected inequality between two metadata hashcode instances.", m9.equals( m10 ) );
        
        
        // Other type check
        assertFalse( "Unexpected equality between two metadata hashcodes.",
                     m6.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}

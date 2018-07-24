package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;

/**
 * Tests the {@link MetricOutputMetadata}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricOutputMetadataTest
{

    /**
     * Test {@link MetricOutputMetadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        TimeWindow firstWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                                           ReferenceTime.ISSUE_TIME );
        Location locationBase = MetadataFactory.getLocation( "DRRC3" );
        Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                     MetadataFactory.getDatasetIdentifier( locationBase,
                                                                                           "SQIN",
                                                                                           "HEFS" ),
                                                     firstWindow );
        MetricOutputMetadata first = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        null );
        MetricOutputMetadata second = MetadataFactory.getOutputMetadata( 1,
                                                                         MetadataFactory.getDimension( "CMS" ),
                                                                         base,
                                                                         MetricConstants.BIAS_FRACTION,
                                                                         null );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( first ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( second ) );
        assertTrue( "Unexpected inequality between two metadata instances.", second.equals( first ) );
        // Transitive
        MetricOutputMetadata secondT = MetadataFactory.getOutputMetadata( 1,
                                                                          MetadataFactory.getDimension( "CMS" ),
                                                                          base,
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
        MetricOutputMetadata third = MetadataFactory.getOutputMetadata( 2,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( third ) );
        MetricOutputMetadata fourth = MetadataFactory.getOutputMetadata( 1,
                                                                         MetadataFactory.getDimension( "CFS" ),
                                                                         base,
                                                                         MetricConstants.BIAS_FRACTION,
                                                                         null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( fourth ) );
        MetricOutputMetadata fifth = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( fifth ) );
        MetricOutputMetadata sixth = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( sixth ) );
        // Unequal input dimensions
        Location seventhLocation = MetadataFactory.getLocation( "DRRC3" );
        MetricOutputMetadata seventh = MetadataFactory.getOutputMetadata( 2,
                                                                          MetadataFactory.getDimension( "CMS" ),
                                                                          MetadataFactory.getMetadata( MetadataFactory.getDimension( "OTHER_DIM" ),
                                                                                                       MetadataFactory.getDatasetIdentifier( seventhLocation,
                                                                                                                                             "SQIN",
                                                                                                                                             "HEFS" ),
                                                                                                       firstWindow ),
                                                                          MetricConstants.BIAS_FRACTION,
                                                                          null );
        assertFalse( "Unexpected equality between two metadata instances.", third.equals( seventh ) );
        // Null check
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( null ) );
        // Other type check
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link MetricOutputMetadata#minimumEquals(MetricOutputMetadata)}.
     */

    @Test
    public void testMinimumEquals()
    {
        Location locationBase = MetadataFactory.getLocation( "DRRC3" );
        Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                     MetadataFactory.getDatasetIdentifier( locationBase,
                                                                                           "SQIN",
                                                                                           "HEFS" ) );
        MetricOutputMetadata first = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        null );
        MetricOutputMetadata second = MetadataFactory.getOutputMetadata( 2,
                                                                         MetadataFactory.getDimension( "CMS" ),
                                                                         base,
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
        MetricOutputMetadata secondT = MetadataFactory.getOutputMetadata( 1,
                                                                          MetadataFactory.getDimension( "CMS" ),
                                                                          base,
                                                                          MetricConstants.BIAS_FRACTION,
                                                                          null );

        assertTrue( "Unexpected inequality between two metadata instances.", second.minimumEquals( secondT ) );
        assertTrue( "Unexpected inequality between two metadata instances.", first.minimumEquals( secondT ) );
        // Unequal
        MetricOutputMetadata third = MetadataFactory.getOutputMetadata( 2,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.minimumEquals( third ) );
        MetricOutputMetadata fourth = MetadataFactory.getOutputMetadata( 2,
                                                                         MetadataFactory.getDimension( "CMS" ),
                                                                         base,
                                                                         MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                         MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", third.minimumEquals( fourth ) );
        MetricOutputMetadata fifth = MetadataFactory.getOutputMetadata( 2,
                                                                        MetadataFactory.getDimension( "CFS" ),
                                                                        base,
                                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                        MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", fourth.minimumEquals( fifth ) );
        Location secondLocation = MetadataFactory.getLocation( "DRRC3" );
        Metadata baseSecond = MetadataFactory.getMetadata( MetadataFactory.getDimension( "OTHER_DIM" ),
                                                           MetadataFactory.getDatasetIdentifier( secondLocation,
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );

        MetricOutputMetadata sixth = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        baseSecond,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        null );
        assertFalse( "Unexpected equality between two metadata instances.", first.minimumEquals( sixth ) );


    }

    /**
     * Test {@link MetricOutputMetadata#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal
        TimeWindow firstWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                                           ReferenceTime.ISSUE_TIME );
        Location baseLocation = MetadataFactory.getLocation( "DRRC3" );
        Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                     MetadataFactory.getDatasetIdentifier( baseLocation,
                                                                                           "SQIN",
                                                                                           "HEFS" ),
                                                     firstWindow );
        MetricOutputMetadata first = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        null );
        MetricOutputMetadata second = MetadataFactory.getOutputMetadata( 1,
                                                                         MetadataFactory.getDimension( "CMS" ),
                                                                         base,
                                                                         MetricConstants.BIAS_FRACTION,
                                                                         null );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == first.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == second.hashCode() );
        MetricOutputMetadata secondT = MetadataFactory.getOutputMetadata( 1,
                                                                          MetadataFactory.getDimension( "CMS" ),
                                                                          base,
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
        MetricOutputMetadata third = MetadataFactory.getOutputMetadata( 2,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == third.hashCode() );
        MetricOutputMetadata fourth = MetadataFactory.getOutputMetadata( 1,
                                                                         MetadataFactory.getDimension( "CFS" ),
                                                                         base,
                                                                         MetricConstants.BIAS_FRACTION,
                                                                         null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == fourth.hashCode() );
        MetricOutputMetadata fifth = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                        null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == fifth.hashCode() );
        MetricOutputMetadata sixth = MetadataFactory.getOutputMetadata( 1,
                                                                        MetadataFactory.getDimension( "CMS" ),
                                                                        base,
                                                                        MetricConstants.BIAS_FRACTION,
                                                                        MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == sixth.hashCode() );
        // Unequal input dimensions
        Location seventhLocation = MetadataFactory.getLocation( "DRRC3" );
        MetricOutputMetadata seventh = MetadataFactory.getOutputMetadata( 2,
                                                                          MetadataFactory.getDimension( "CMS" ),
                                                                          MetadataFactory.getMetadata( MetadataFactory.getDimension( "OTHER_DIM" ),
                                                                                                       MetadataFactory.getDatasetIdentifier( seventhLocation,
                                                                                                                                             "SQIN",
                                                                                                                                             "HEFS" ),
                                                                                                       firstWindow ),
                                                                          MetricConstants.BIAS_FRACTION,
                                                                          null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", third.hashCode() == seventh.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two metadata hashcodes.",
                     first.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}

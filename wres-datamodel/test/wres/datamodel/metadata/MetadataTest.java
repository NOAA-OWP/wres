package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.DataFactory;

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
                    MetadataFactory.getMetadata().equals( MetadataFactory.getMetadata() ) );
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m1 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                   MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" ) );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m1 ) );
        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m2 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                   MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m2.equals( m1 ) );
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m3 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" ) );
        Location l4 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m4 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l4, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m1.equals( m3 ) );
        // Transitive
        Location l4t = MetadataFactory.getLocation( "DRRC2" );
        Metadata m4t = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                    MetadataFactory.getDatasetIdentifier( l4t, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.equals( m4t ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4t ) );
        // Unequal
        Location l5 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m5 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l5, "SQIN", "HEFS" ) );
        assertFalse( "Unexpected equality between two metadata instances.", m4.equals( m5 ) );
        Metadata m5NoDim = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ), null );
        assertFalse( "Unexpected equality between two metadata instances.", m5.equals( m5NoDim ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        }
        // Add a time window
        TimeWindow firstWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                                           ReferenceTime.VALID_TIME );
        TimeWindow secondWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                            Instant.parse( "1986-01-01T00:00:00Z" ),
                                                            ReferenceTime.VALID_TIME );
        Location l6 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m6 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l6, "SQIN", "HEFS" ),
                                                   firstWindow );
        Location l7 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m7 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l7, "SQIN", "HEFS" ),
                                                   secondWindow );
        assertTrue( "Unexpected inequality between two metadata instances.", m6.equals( m7 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m7.equals( m6 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m3.equals( m6 ) );
        TimeWindow thirdWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                                           ReferenceTime.ISSUE_TIME );
        Location l8 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m8 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l8, "SQIN", "HEFS" ),
                                                   thirdWindow );
        assertFalse( "Unexpected equality between two metadata instances.", m6.equals( m8 ) );
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
                    MetadataFactory.getMetadata().hashCode() == MetadataFactory.getMetadata().hashCode() );
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m1 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                   MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m1.hashCode() == m1.hashCode() );
        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m2 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                   MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m3 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" ) );
        Location l4 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m4 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l4, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m3.hashCode() == m4.hashCode() );
        Location l4t = MetadataFactory.getLocation( "DRRC2" );
        Metadata m4t = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                    MetadataFactory.getDatasetIdentifier( l4t, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.hashCode() == m4t.hashCode() );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.hashCode() == m4t.hashCode() );
        // Unequal
        assertFalse( "Unexpected equality between two metadata hashcodes.", m1.hashCode() == m3.hashCode() );
        Location l5 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m5 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l5, "SQIN", "HEFS" ) );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m4.hashCode() == m5.hashCode() );
        Metadata m5NoDim = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ), null );
        assertFalse( "Unexpected equality between two metadata instances.", m5.hashCode() == m5NoDim.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        }

        // Add a time window
        TimeWindow firstWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                                           ReferenceTime.VALID_TIME );
        TimeWindow secondWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                            Instant.parse( "1986-01-01T00:00:00Z" ),
                                                            ReferenceTime.VALID_TIME );
        Location l6 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m6 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l6, "SQIN", "HEFS" ),
                                                   firstWindow );
        Location l7 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m7 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l7, "SQIN", "HEFS" ),
                                                   secondWindow );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m6.hashCode() == m7.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m7.hashCode() == m6.hashCode() );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m3.hashCode() == m6.hashCode() );
        TimeWindow thirdWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1986-01-01T00:00:00Z" ),
                                                           ReferenceTime.ISSUE_TIME );
        Location l8 = MetadataFactory.getLocation( "DRRC3" );
        Metadata m8 = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                                   MetadataFactory.getDatasetIdentifier( l8, "SQIN", "HEFS" ),
                                                   thirdWindow );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m6.hashCode() == m8.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two metadata hashcodes.",
                     m6.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}

package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link DatasetIdentifier}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DatasetIdentifierTest
{

    /**
     * Test {@link DatasetIdentifier#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive
        Location l1 = Location.of( "DRRC2" );
        DatasetIdentifier m1 = DatasetIdentifier.of( l1, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m1 ) );

        Location l2 = Location.of( "DRRC2" );
        DatasetIdentifier m2 = DatasetIdentifier.of( l2, "SQIN", "HEFS" );

        // Symmetric
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m1 ) );

        // Transitive
        Location l3 = Location.of( "DRRC2" );
        DatasetIdentifier m3 = DatasetIdentifier.of( l3, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        }

        // Equal with some identifiers missing
        Location lp1 = Location.of( "DRRC2" );
        DatasetIdentifier p1 = DatasetIdentifier.of( lp1, "SQIN", null );

        Location lp2 = Location.of( "DRRC2" );
        DatasetIdentifier p2 = DatasetIdentifier.of( lp2, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p1.equals( p2 ) );

        Location lp3 = Location.of( "DRRC2" );
        DatasetIdentifier p3 = DatasetIdentifier.of( lp3, null, null );

        Location lp4 = Location.of( "DRRC2" );
        DatasetIdentifier p4 = DatasetIdentifier.of( lp4, null, null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p3.equals( p4 ) );

        DatasetIdentifier p5 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p6 = DatasetIdentifier.of( null, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p5.equals( p6 ) );

        // Equal with scenario identifier for baseline
        Location lb1 = Location.of( "DRRC2" );
        final Location geospatialID = lb1;
        DatasetIdentifier b1 = DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP" );

        Location lb2 = Location.of( "DRRC2" );
        final Location geospatialID1 = lb2;
        DatasetIdentifier b2 = DatasetIdentifier.of( geospatialID1, "SQIN", "HEFS", "ESP" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", b1.equals( b2 ) );

        // Unequal
        Location l4 = Location.of( "DRRC3" );
        DatasetIdentifier m4 = DatasetIdentifier.of( l4, "SQIN", "HEFS" );

        Location l5 = Location.of( "DRRC2" );
        DatasetIdentifier m5 = DatasetIdentifier.of( l5, "SQIN2", "HEFS" );

        Location l6 = Location.of( "DRRC2" );
        DatasetIdentifier m6 = DatasetIdentifier.of( l6, "SQIN", "HEFS4" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m4 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m5 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m6 ) );

        // Unequal with some identifiers missing
        Location lp7 = Location.of( "DRRC2" );
        DatasetIdentifier p7 = DatasetIdentifier.of( lp7, "SQIN", null );

        Location lp8 = Location.of( "DRRC2" );
        DatasetIdentifier p8 = DatasetIdentifier.of( lp8, "SQIN", "HEFS" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p7.equals( p8 ) );

        Location lp9 = Location.of( "DRRC2" );
        DatasetIdentifier p9 = DatasetIdentifier.of( lp9, null, null );

        DatasetIdentifier p10 = DatasetIdentifier.of( null, "VAR", null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p9.equals( p10 ) );

        DatasetIdentifier p11 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p12 = DatasetIdentifier.of( Location.of( "AB" ), null, null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p11.equals( p12 ) );

        // Unequal scenario identifiers for baseline
        Location lb3 = Location.of( "DRRC2" );
        final Location geospatialID2 = lb3;
        DatasetIdentifier b3 = DatasetIdentifier.of( geospatialID2, "SQIN", "HEFS", "ESP" );

        Location lb4 = Location.of( "DRRC2" );
        final Location geospatialID3 = lb4;
        DatasetIdentifier b4 = DatasetIdentifier.of( geospatialID3, "SQIN", "HEFS", "ESP2" );

        Location lb5 = Location.of( "DRRC2" );
        final Location geospatialID4 = lb5;
        DatasetIdentifier b5 = DatasetIdentifier.of( geospatialID4, "SQIN", "HEFS", null );

        assertFalse( "Unexpected inequality between two dataset identifier instances.", b3.equals( b4 ) );
        assertFalse( "Unexpected inequality between two dataset identifier instances.", p1.equals( b3 ) );
        assertFalse( "Unexpected inequality between two dataset identifier instances.", b5.equals( b3 ) );
        assertFalse( "Unexpected inequality between two dataset identifier instances.", b3.equals( b5 ) );

        // Null check
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( null ) );

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     m1.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link DatasetIdentifier#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal
        Location l1 = Location.of( "DRRC2" );
        DatasetIdentifier m1 = DatasetIdentifier.of( l1, "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m1.hashCode() );
        Location l2 = Location.of( "DRRC2" );
        DatasetIdentifier m2 = DatasetIdentifier.of( l2, "SQIN", "HEFS" );
        Location l3 = Location.of( "DRRC2" );
        DatasetIdentifier m3 = DatasetIdentifier.of( l3, "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m2.hashCode() );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m2.hashCode() == m3.hashCode() );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m3.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.",
                        m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        Location l4 = Location.of( "DRRC3" );
        DatasetIdentifier m4 = DatasetIdentifier.of( l4, "SQIN", "HEFS" );
        Location l5 = Location.of( "DRRC2" );
        DatasetIdentifier m5 = DatasetIdentifier.of( l5, "SQIN2", "HEFS" );
        Location l6 = Location.of( "DRRC2" );
        DatasetIdentifier m6 = DatasetIdentifier.of( l6, "SQIN", "HEFS4" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m4.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m5.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m6.hashCode() );

        // Equal with some identifiers missing
        Location lp1 = Location.of( "DRRC2" );
        DatasetIdentifier p1 = DatasetIdentifier.of( lp1, "SQIN", null );
        Location lp2 = Location.of( "DRRC2" );
        DatasetIdentifier p2 = DatasetIdentifier.of( lp2, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p1.hashCode() == p2.hashCode() );
        Location lp3 = Location.of( "DRRC2" );
        DatasetIdentifier p3 = DatasetIdentifier.of( lp3, null, null );
        Location lp4 = Location.of( "DRRC2" );
        DatasetIdentifier p4 = DatasetIdentifier.of( lp4, null, null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p3.hashCode() == p4.hashCode() );
        DatasetIdentifier p5 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p6 = DatasetIdentifier.of( null, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p5.hashCode() == p6.hashCode() );
        // Equal with scenario identifier for baseline
        Location lb1 = Location.of( "DRRC2" );
        final Location geospatialID = lb1;
        DatasetIdentifier b1 = DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP" );
        Location lb2 = Location.of( "DRRC2" );
        final Location geospatialID1 = lb2;
        DatasetIdentifier b2 = DatasetIdentifier.of( geospatialID1, "SQIN", "HEFS", "ESP" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", b1.hashCode() == b2.hashCode() );
        // Unequal with some identifiers missing
        Location lp7 = Location.of( "DRRC2" );
        DatasetIdentifier p7 = DatasetIdentifier.of( lp7, "SQIN", null );
        Location lp8 = Location.of( "DRRC2" );
        DatasetIdentifier p8 = DatasetIdentifier.of( lp8, "SQIN", "HEFS" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p7.hashCode() == p8.hashCode() );
        Location lp9 = Location.of( "DRRC2" );
        DatasetIdentifier p9 = DatasetIdentifier.of( lp9, null, null );
        DatasetIdentifier p10 = DatasetIdentifier.of( null, "SQIN", null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p9.hashCode() == p10.hashCode() );
        DatasetIdentifier p11 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p12 =
                DatasetIdentifier.of( Location.of( "LOC" ), null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     p11.hashCode() == p12.hashCode() );
        // Unequal scenario identifiers for baseline
        Location lb3 = Location.of( "DRRC2" );
        final Location geospatialID2 = lb3;
        DatasetIdentifier b3 = DatasetIdentifier.of( geospatialID2, "SQIN", "HEFS", "ESP" );
        Location lb4 = Location.of( "DRRC2" );
        final Location geospatialID3 = lb4;
        DatasetIdentifier b4 = DatasetIdentifier.of( geospatialID3, "SQIN", "HEFS", "ESP2" );
        Location lb5 = Location.of( "DRRC2" );
        final Location geospatialID4 = lb5;
        DatasetIdentifier b5 = DatasetIdentifier.of( geospatialID4, "SQIN", "HEFS", null );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.",
                     b3.hashCode() == b4.hashCode() );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.",
                     p1.hashCode() == b3.hashCode() );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.",
                     b3.hashCode() == b5.hashCode() );

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}

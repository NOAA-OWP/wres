package wres.datamodel.sampledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;

/**
 * Tests the {@link DatasetIdentifier}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DatasetIdentifierTest
{

    private static final String DRRC2 = "DRRC2";

    /**
     * Test {@link DatasetIdentifier#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive
        Location l1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m1 = DatasetIdentifier.of( l1, "SQIN", "HEFS" );

        assertEquals( m1, m1 );

        Location l2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m2 = DatasetIdentifier.of( l2, "SQIN", "HEFS" );

        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );

        // Transitive
        Location l3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m3 = DatasetIdentifier.of( l3, "SQIN", "HEFS" );

        assertEquals( m2, m3 );
        assertEquals( m1, m3 );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( m1, m2 );
        }

        // Equal with some identifiers missing
        Location lp1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p1 = DatasetIdentifier.of( lp1, "SQIN", null );

        Location lp2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p2 = DatasetIdentifier.of( lp2, "SQIN", null );

        assertEquals( p1, p2 );

        Location lp3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p3 = DatasetIdentifier.of( lp3, null, null );

        Location lp4 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p4 = DatasetIdentifier.of( lp4, null, null );

        assertEquals( p3, p4 );

        DatasetIdentifier p5 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p6 = DatasetIdentifier.of( null, "SQIN", null );

        assertEquals( p5, p6 );

        // Equal with scenario identifier for baseline
        Location lb1 = FeatureKey.of( DRRC2 );
        final Location geospatialID = lb1;
        DatasetIdentifier b1 = DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP" );

        Location lb2 = FeatureKey.of( DRRC2 );
        final Location geospatialID1 = lb2;
        DatasetIdentifier b2 = DatasetIdentifier.of( geospatialID1, "SQIN", "HEFS", "ESP" );

        assertEquals( b1, b2 );

        // Equal with LRB context
        DatasetIdentifier r3 =
                DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.BASELINE );
        DatasetIdentifier r4 =
                DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.BASELINE );

        assertEquals( r3, r4 );

        // Unequal
        Location l4 = FeatureKey.of( "DRRC3" );
        DatasetIdentifier m4 = DatasetIdentifier.of( l4, "SQIN", "HEFS" );

        Location l5 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m5 = DatasetIdentifier.of( l5, "SQIN2", "HEFS" );

        Location l6 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m6 = DatasetIdentifier.of( l6, "SQIN", "HEFS4" );

        assertNotEquals( m1, m4 );
        assertNotEquals( m1, m5 );
        assertNotEquals( m1, m6 );

        // Unequal with some identifiers missing
        Location lp7 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p7 = DatasetIdentifier.of( lp7, "SQIN", null );

        Location lp8 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p8 = DatasetIdentifier.of( lp8, "SQIN", "HEFS" );

        assertNotEquals( p7, p8 );

        Location lp9 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p9 = DatasetIdentifier.of( lp9, null, null );

        DatasetIdentifier p10 = DatasetIdentifier.of( null, "VAR", null );

        assertNotEquals( p9, p10 );

        DatasetIdentifier p11 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p12 = DatasetIdentifier.of( FeatureKey.of( "AB" ), null, null );

        assertNotEquals( p11, p12 );

        // Unequal scenario identifiers for baseline
        Location lb3 = FeatureKey.of( DRRC2 );
        final Location geospatialID2 = lb3;
        DatasetIdentifier b3 = DatasetIdentifier.of( geospatialID2, "SQIN", "HEFS", "ESP" );

        Location lb4 = FeatureKey.of( DRRC2 );
        final Location geospatialID3 = lb4;
        DatasetIdentifier b4 = DatasetIdentifier.of( geospatialID3, "SQIN", "HEFS", "ESP2" );

        Location lb5 = FeatureKey.of( DRRC2 );
        final Location geospatialID4 = lb5;
        DatasetIdentifier b5 = DatasetIdentifier.of( geospatialID4, "SQIN", "HEFS", null );

        assertNotEquals( b3, b4 );
        assertNotEquals( p1, b3 );
        assertNotEquals( b5, b3 );
        assertNotEquals( b3, b5 );

        // Unequal on LRB
        assertNotEquals( b2, r3 );
        DatasetIdentifier r5 = DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.LEFT );
        assertNotEquals( b4, r5 );

        // Null check
        assertNotEquals( null, m1 );

        // Other type check
        assertNotEquals( Double.valueOf( 2 ), m1 );
    }

    /**
     * Test {@link DatasetIdentifier#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal
        Location l1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m1 = DatasetIdentifier.of( l1, "SQIN", "HEFS" );
        assertEquals( m1.hashCode(), m1.hashCode() );
        Location l2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m2 = DatasetIdentifier.of( l2, "SQIN", "HEFS" );
        Location l3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m3 = DatasetIdentifier.of( l3, "SQIN", "HEFS" );
        assertEquals( m1.hashCode(), m2.hashCode() );
        assertEquals( m2.hashCode(), m3.hashCode() );
        assertEquals( m1.hashCode(), m3.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( "Unexpected inequality between two dataset identifier instances.",
                        m1.hashCode(), m2.hashCode() );
        }

        // Equal with some identifiers missing
        Location lp1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p1 = DatasetIdentifier.of( lp1, "SQIN", null );
        Location lp2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p2 = DatasetIdentifier.of( lp2, "SQIN", null );
        assertEquals( p1.hashCode(), p2.hashCode() );
        Location lp3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p3 = DatasetIdentifier.of( lp3, null, null );
        Location lp4 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p4 = DatasetIdentifier.of( lp4, null, null );
        assertEquals( p3.hashCode(), p4.hashCode() );
        DatasetIdentifier p5 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p6 = DatasetIdentifier.of( null, "SQIN", null );
        assertEquals( p5.hashCode(), p6.hashCode() );

        // Equal with scenario identifier for baseline
        Location lb1 = FeatureKey.of( DRRC2 );
        final Location geospatialID = lb1;
        DatasetIdentifier b1 = DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP" );
        Location lb2 = FeatureKey.of( DRRC2 );
        final Location geospatialID1 = lb2;
        DatasetIdentifier b2 = DatasetIdentifier.of( geospatialID1, "SQIN", "HEFS", "ESP" );
        assertEquals( b1.hashCode(), b2.hashCode() );

        // Equal with LRB
        DatasetIdentifier r1 = DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );
        DatasetIdentifier r2 =
                DatasetIdentifier.of( geospatialID1, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );
        assertEquals( r1.hashCode(), r2.hashCode() );

        // Other type check
        assertNotEquals( "Unexpected equality between two dataset identifier hashcodes.",
                         m1.hashCode(),
                         Double.valueOf( 2 ).hashCode() );
    }

}

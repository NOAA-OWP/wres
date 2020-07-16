package wres.datamodel.sampledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;

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
        FeatureKey l1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m1 = DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ), "SQIN", "HEFS" );

        assertEquals( m1, m1 );

        FeatureKey l2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m2 = DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ), "SQIN", "HEFS" );

        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );

        // Transitive
        FeatureKey l3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m3 = DatasetIdentifier.of( new FeatureTuple( l3, l3, l3 ), "SQIN", "HEFS" );

        assertEquals( m2, m3 );
        assertEquals( m1, m3 );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( m1, m2 );
        }

        // Equal with some identifiers missing
        FeatureKey lp1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p1 = DatasetIdentifier.of( new FeatureTuple( lp1, lp1, lp1 ), "SQIN", null );

        FeatureKey lp2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p2 = DatasetIdentifier.of( new FeatureTuple( lp2, lp2, lp2 ), "SQIN", null );

        assertEquals( p1, p2 );

        FeatureKey lp3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p3 = DatasetIdentifier.of( new FeatureTuple( lp3, lp3, lp3 ), null, null );

        FeatureKey lp4 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p4 = DatasetIdentifier.of( new FeatureTuple( lp4, lp4, lp4 ), null, null );

        assertEquals( p3, p4 );

        DatasetIdentifier p5 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p6 = DatasetIdentifier.of( null, "SQIN", null );

        assertEquals( p5, p6 );

        // Equal with scenario identifier for baseline
        FeatureKey lb1 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID = lb1;
        DatasetIdentifier b1 = DatasetIdentifier.of( new FeatureTuple( geospatialID, geospatialID, geospatialID ), "SQIN", "HEFS", "ESP" );

        FeatureKey lb2 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID1 = lb2;
        DatasetIdentifier b2 = DatasetIdentifier.of( new FeatureTuple( geospatialID1, geospatialID1, geospatialID1 ), "SQIN", "HEFS", "ESP" );

        assertEquals( b1, b2 );

        // Equal with LRB context
        DatasetIdentifier r3 =
                DatasetIdentifier.of( new FeatureTuple( geospatialID, geospatialID, geospatialID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.BASELINE );
        DatasetIdentifier r4 =
                DatasetIdentifier.of( new FeatureTuple( geospatialID, geospatialID, geospatialID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.BASELINE );

        assertEquals( r3, r4 );

        // Unequal
        FeatureKey l4 = FeatureKey.of( "DRRC3" );
        DatasetIdentifier m4 = DatasetIdentifier.of( new FeatureTuple( l4, l4, l4 ), "SQIN", "HEFS" );

        FeatureKey l5 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m5 = DatasetIdentifier.of( new FeatureTuple( l5, l5, l5 ), "SQIN2", "HEFS" );

        FeatureKey l6 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m6 = DatasetIdentifier.of( new FeatureTuple( l6, l6, l6 ), "SQIN", "HEFS4" );

        assertNotEquals( m1, m4 );
        assertNotEquals( m1, m5 );
        assertNotEquals( m1, m6 );

        // Unequal with some identifiers missing
        FeatureKey lp7 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p7 = DatasetIdentifier.of( new FeatureTuple( lp7, lp7, lp7 ), "SQIN", null );

        FeatureKey lp8 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p8 = DatasetIdentifier.of( new FeatureTuple( lp8, lp8, lp8 ), "SQIN", "HEFS" );

        assertNotEquals( p7, p8 );

        FeatureKey lp9 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p9 = DatasetIdentifier.of( new FeatureTuple( lp9, lp9, lp9 ), null, null );

        DatasetIdentifier p10 = DatasetIdentifier.of( null, "VAR", null );

        assertNotEquals( p9, p10 );

        DatasetIdentifier p11 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p12 = DatasetIdentifier.of( new FeatureTuple( FeatureKey.of( "AB" ), FeatureKey.of( "AB" ), FeatureKey.of( "AB" ) ), null, null );

        assertNotEquals( p11, p12 );

        // Unequal scenario identifiers for baseline
        FeatureKey lb3 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID2 = lb3;
        DatasetIdentifier b3 = DatasetIdentifier.of( new FeatureTuple( geospatialID2, geospatialID2, geospatialID2 ), "SQIN", "HEFS", "ESP" );

        FeatureKey lb4 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID3 = lb4;
        DatasetIdentifier b4 = DatasetIdentifier.of( new FeatureTuple( geospatialID3, geospatialID3, geospatialID3 ), "SQIN", "HEFS", "ESP2" );

        FeatureKey lb5 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID4 = lb5;
        DatasetIdentifier b5 = DatasetIdentifier.of( new FeatureTuple( geospatialID4, geospatialID4, geospatialID4 ), "SQIN", "HEFS", null );

        assertNotEquals( b3, b4 );
        assertNotEquals( p1, b3 );
        assertNotEquals( b5, b3 );
        assertNotEquals( b3, b5 );

        // Unequal on LRB
        assertNotEquals( b2, r3 );
        DatasetIdentifier r5 = DatasetIdentifier.of( new FeatureTuple( geospatialID, geospatialID, geospatialID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.LEFT );
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
        FeatureKey l1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m1 = DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ), "SQIN", "HEFS" );
        assertEquals( m1.hashCode(), m1.hashCode() );
        FeatureKey l2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m2 = DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ), "SQIN", "HEFS" );
        FeatureKey l3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier m3 = DatasetIdentifier.of( new FeatureTuple( l3, l3, l3 ), "SQIN", "HEFS" );
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
        FeatureKey lp1 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p1 = DatasetIdentifier.of( new FeatureTuple( lp1, lp1, lp1 ), "SQIN", null );
        FeatureKey lp2 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p2 = DatasetIdentifier.of( new FeatureTuple( lp2, lp2, lp2 ), "SQIN", null );
        assertEquals( p1.hashCode(), p2.hashCode() );
        FeatureKey lp3 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p3 = DatasetIdentifier.of( new FeatureTuple( lp3, lp3, lp3 ), null, null );
        FeatureKey lp4 = FeatureKey.of( DRRC2 );
        DatasetIdentifier p4 = DatasetIdentifier.of( new FeatureTuple( lp4, lp4, lp4 ), null, null );
        assertEquals( p3.hashCode(), p4.hashCode() );
        DatasetIdentifier p5 = DatasetIdentifier.of( null, "SQIN", null );
        DatasetIdentifier p6 = DatasetIdentifier.of( null, "SQIN", null );
        assertEquals( p5.hashCode(), p6.hashCode() );

        // Equal with scenario identifier for baseline
        FeatureKey lb1 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID = lb1;
        DatasetIdentifier b1 = DatasetIdentifier.of( new FeatureTuple( geospatialID, geospatialID, geospatialID ), "SQIN", "HEFS", "ESP" );
        FeatureKey lb2 = FeatureKey.of( DRRC2 );
        final FeatureKey geospatialID1 = lb2;
        DatasetIdentifier b2 = DatasetIdentifier.of( new FeatureTuple( geospatialID1, geospatialID1, geospatialID1 ), "SQIN", "HEFS", "ESP" );
        assertEquals( b1.hashCode(), b2.hashCode() );

        // Equal with LRB
        DatasetIdentifier r1 = DatasetIdentifier.of( new FeatureTuple( geospatialID, geospatialID, geospatialID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );
        DatasetIdentifier r2 =
                DatasetIdentifier.of( new FeatureTuple( geospatialID1, geospatialID1, geospatialID1 ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );
        assertEquals( r1.hashCode(), r2.hashCode() );

        // Other type check
        assertNotEquals( "Unexpected equality between two dataset identifier hashcodes.",
                         m1.hashCode(),
                         Double.valueOf( 2 ).hashCode() );
    }

}

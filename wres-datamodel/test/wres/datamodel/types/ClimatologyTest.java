package wres.datamodel.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import wres.datamodel.space.Feature;
import wres.statistics.MessageUtilities;

/**
 * Tests the {@link Climatology}.
 *
 * @author James Brown
 */

class ClimatologyTest
{
    /** Measurement unit. */
    private static final String UNIT = "bar";

    @Test
    void testMergeOnBuild()
    {
        Climatology.Builder builder = new Climatology.Builder();
        Feature feature = Feature.of( MessageUtilities.getGeometry( "foo" ) );
        builder.addClimatology( feature, new double[] { 1 }, UNIT );
        builder.addClimatology( feature, new double[] { 2 }, UNIT );

        Climatology actual = builder.build();
        Climatology expected = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                                        .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGet()
    {
        Feature feature = Feature.of( MessageUtilities.getGeometry( "foo" ) );
        Climatology climatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                                           .build();

        double[] actual = climatology.get( feature );
        double[] expected = new double[] { 1, 2 };

        assertArrayEquals( expected, actual );

        // Not identity equals
        assertNotSame( actual, expected );
    }

    @Test
    void testGetFeatures()
    {
        Feature fooFeature = Feature.of( MessageUtilities.getGeometry( "foo" ) );
        Feature barFeature = Feature.of( MessageUtilities.getGeometry( "bar" ) );
        Climatology climatology = new Climatology.Builder().addClimatology( fooFeature, new double[] { 1, 2 }, UNIT )
                                                           .addClimatology( barFeature, new double[] { 3, 4 }, UNIT )
                                                           .build();

        Set<Feature> actual = climatology.getFeatures();
        Set<Feature> expected = Set.of( fooFeature, barFeature );

        assertEquals( expected, actual );
    }

    @Test
    void testEquals()
    {
        Feature feature = Feature.of( MessageUtilities.getGeometry( "foo" ) );

        // Reflexive 
        Climatology climatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                                           .build();

        assertEquals( climatology, climatology );

        // Symmetric
        Climatology otherClimatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                                                .build();
        assertTrue( climatology.equals( otherClimatology ) && otherClimatology.equals( climatology ) );

        // Transitive
        Climatology oneMoreClimatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                         .build();
        assertTrue( climatology.equals( otherClimatology ) && otherClimatology.equals( oneMoreClimatology )
                    && climatology.equals( oneMoreClimatology ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( climatology, otherClimatology );
        }

        // Nullity
        assertNotEquals( null, climatology );

        // Check unequal cases
        Climatology yetAnotherClimatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1 }, UNIT )
                                         .build();

        assertNotEquals( climatology, yetAnotherClimatology );

        Feature anotheFeature = Feature.of( MessageUtilities.getGeometry( "bar" ) );
        Climatology oneFinalClimatology =
                new Climatology.Builder().addClimatology( anotheFeature, new double[] { 1 }, UNIT )
                                         .build();

        assertNotEquals( yetAnotherClimatology, oneFinalClimatology );
    }

    @Test
    void testHashCode()
    {
        Feature feature = Feature.of( MessageUtilities.getGeometry( "foo" ) );

        Climatology climatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                                           .build();

        Climatology otherClimatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 }, UNIT )
                                                                .build();

        // Consistent with equals
        assertTrue( climatology.equals( otherClimatology ) && climatology.hashCode() == otherClimatology.hashCode() );

        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( climatology.hashCode(), otherClimatology.hashCode() );
        }
    }

}

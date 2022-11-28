package wres.datamodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;

import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.Feature;

/**
 * Tests the {@link Climatology}.
 * 
 * @author James Brown
 */

class ClimatologyTest
{
    @Test
    void testMergeOnBuild()
    {
        Climatology.Builder builder = new Climatology.Builder();
        Feature feature = Feature.of( MessageFactory.getGeometry( "foo" ) );
        builder.addClimatology( feature, new double[] { 1 } );
        builder.addClimatology( feature, new double[] { 2 } );

        Climatology actual = builder.build();
        Climatology expected = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                        .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGet()
    {
        Feature feature = Feature.of( MessageFactory.getGeometry( "foo" ) );
        Climatology climatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                           .build();

        double[] actual = climatology.get( feature );
        double[] expected = new double[] { 1, 2 };

        assertTrue( Arrays.equals( expected, actual ) );

        // Not identity equals
        assertFalse( actual == expected );
    }

    @Test
    void testGetFeatures()
    {
        Feature fooFeature = Feature.of( MessageFactory.getGeometry( "foo" ) );
        Feature barFeature = Feature.of( MessageFactory.getGeometry( "bar" ) );
        Climatology climatology = new Climatology.Builder().addClimatology( fooFeature, new double[] { 1, 2 } )
                                                           .addClimatology( barFeature, new double[] { 3, 4 } )
                                                           .build();

        Set<Feature> actual = climatology.getFeatures();
        Set<Feature> expected = Set.of( fooFeature, barFeature );

        assertEquals( expected, actual );
    }

    @Test
    void testEquals()
    {
        Feature feature = Feature.of( MessageFactory.getGeometry( "foo" ) );

        // Reflexive 
        Climatology climatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                           .build();

        assertTrue( climatology.equals( climatology ) );

        // Symmetric
        Climatology otherClimatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                                .build();
        assertTrue( climatology.equals( otherClimatology ) && otherClimatology.equals( climatology ) );

        // Transitive
        Climatology oneMoreClimatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                                  .build();
        assertTrue( climatology.equals( otherClimatology ) && otherClimatology.equals( oneMoreClimatology )
                    && climatology.equals( oneMoreClimatology ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( climatology.equals( otherClimatology ) );
        }

        // Nullity
        assertNotEquals( null, climatology );

        // Check unequal cases
        Climatology yetAnotherClimatology = new Climatology.Builder().addClimatology( feature, new double[] { 1 } )
                                                                     .build();

        assertFalse( climatology.equals( yetAnotherClimatology ) );

        Feature anotheFeature = Feature.of( MessageFactory.getGeometry( "bar" ) );
        Climatology oneFinalClimatology = new Climatology.Builder().addClimatology( anotheFeature, new double[] { 1 } )
                                                                   .build();

        assertFalse( yetAnotherClimatology.equals( oneFinalClimatology ) );
    }

    @Test
    public void testHashCode()
    {
        Feature feature = Feature.of( MessageFactory.getGeometry( "foo" ) );

        Climatology climatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                           .build();

        Climatology otherClimatology = new Climatology.Builder().addClimatology( feature, new double[] { 1, 2 } )
                                                                .build();

        // Consistent with equals
        assertTrue( climatology.equals( otherClimatology ) && climatology.hashCode() == otherClimatology.hashCode() );

        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( climatology.hashCode() == otherClimatology.hashCode() );
        }
    }

}

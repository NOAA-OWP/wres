package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;

import org.junit.Test;

import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataException;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;

/**
 * Tests the {@link DefaultMetadataFactory}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public final class DefaultMetadataFactoryTest
{

    /**
     * Instance of a {@link MetadataFactory}.
     */

    private final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();

    /**
     * Instance of a {@link DataFactory}.
     */

    private final DataFactory dataFac = DefaultDataFactory.getInstance();

    /**
     * Test {@link Metadata#equals(Object)}.
     */

    @Test
    public void metadataEquals()
    {
        assertTrue( "Unexpected inequality between two metadata instances.",
                    metaFac.getMetadata().equals( metaFac.getMetadata() ) );
        Location l1 = metaFac.getLocation( "DRRC2" );
        Metadata m1 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l1, "SQIN", "HEFS" ) );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m1 ) );
        Location l2 = metaFac.getLocation( "DRRC2" );
        Metadata m2 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l2, "SQIN", "HEFS" ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m2.equals( m1 ) );
        Location l3 = metaFac.getLocation( "DRRC2" );
        Metadata m3 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l3, "SQIN", "HEFS" ) );
        Location l4 = metaFac.getLocation( "DRRC2" );
        Metadata m4 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l4, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m1.equals( m3 ) );
        // Transitive
        Location l4t = metaFac.getLocation( "DRRC2" );
        Metadata m4t = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                            metaFac.getDatasetIdentifier( l4t, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.equals( m4t ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4t ) );
        // Unequal
        Location l5 = metaFac.getLocation( "DRRC3" );
        Metadata m5 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l5, "SQIN", "HEFS" ) );
        assertFalse( "Unexpected equality between two metadata instances.", m4.equals( m5 ) );
        Metadata m5NoDim = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ), null );
        assertFalse( "Unexpected equality between two metadata instances.", m5.equals( m5NoDim ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        }
        // Add a time window
        TimeWindow firstWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.VALID_TIME );
        TimeWindow secondWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                        Instant.parse( "1986-01-01T00:00:00Z" ),
                                                        ReferenceTime.VALID_TIME );
        Location l6 = metaFac.getLocation( "DRRC3" );
        Metadata m6 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l6, "SQIN", "HEFS" ),
                                           firstWindow );
        Location l7 = metaFac.getLocation( "DRRC3" );
        Metadata m7 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l7, "SQIN", "HEFS" ),
                                           secondWindow );
        assertTrue( "Unexpected inequality between two metadata instances.", m6.equals( m7 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m7.equals( m6 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m3.equals( m6 ) );
        TimeWindow thirdWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Location l8 = metaFac.getLocation( "DRRC3" );
        Metadata m8 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l8, "SQIN", "HEFS" ),
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
    public void metadataHashcode()
    {
        // Equal
        assertTrue( "Unexpected inequality between two metadata hashcodes.",
                    metaFac.getMetadata().hashCode() == metaFac.getMetadata().hashCode() );
        Location l1 = metaFac.getLocation( "DRRC2" );
        Metadata m1 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l1, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m1.hashCode() == m1.hashCode() );
        Location l2 = metaFac.getLocation( "DRRC2" );
        Metadata m2 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l2, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        Location l3 = metaFac.getLocation( "DRRC2" );
        Metadata m3 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l3, "SQIN", "HEFS" ) );
        Location l4 = metaFac.getLocation( "DRRC2" );
        Metadata m4 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l4, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m3.hashCode() == m4.hashCode() );
        Location l4t = metaFac.getLocation( "DRRC2" );
        Metadata m4t = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                            metaFac.getDatasetIdentifier( l4t, "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.hashCode() == m4t.hashCode() );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.hashCode() == m4t.hashCode() );
        // Unequal
        assertFalse( "Unexpected equality between two metadata hashcodes.", m1.hashCode() == m3.hashCode() );
        Location l5 = metaFac.getLocation( "DRRC3" );
        Metadata m5 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l5, "SQIN", "HEFS" ) );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m4.hashCode() == m5.hashCode() );
        Metadata m5NoDim = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ), null );
        assertFalse( "Unexpected equality between two metadata instances.", m5.hashCode() == m5NoDim.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        }

        // Add a time window
        TimeWindow firstWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.VALID_TIME );
        TimeWindow secondWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                        Instant.parse( "1986-01-01T00:00:00Z" ),
                                                        ReferenceTime.VALID_TIME );
        Location l6 = metaFac.getLocation( "DRRC3" );
        Metadata m6 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l6, "SQIN", "HEFS" ),
                                           firstWindow );
        Location l7 = metaFac.getLocation( "DRRC3" );
        Metadata m7 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l7, "SQIN", "HEFS" ),
                                           secondWindow );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m6.hashCode() == m7.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m7.hashCode() == m6.hashCode() );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m3.hashCode() == m6.hashCode() );
        TimeWindow thirdWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Location l8 = metaFac.getLocation( "DRRC3" );
        Metadata m8 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( l8, "SQIN", "HEFS" ),
                                           thirdWindow );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m6.hashCode() == m8.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two metadata hashcodes.",
                     m6.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

    /**
     * Test {@link MetricOutputMetadata#equals(Object)}.
     */

    @Test
    public void outputMetadataEquals()
    {
        TimeWindow firstWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Location locationBase = metaFac.getLocation( "DRRC3" );
        Metadata base = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                             metaFac.getDatasetIdentifier( locationBase, "SQIN", "HEFS" ),
                                             firstWindow );
        MetricOutputMetadata first = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                null );
        MetricOutputMetadata second = metaFac.getOutputMetadata( 1,
                                                                 metaFac.getDimension( "CMS" ),
                                                                 base,
                                                                 MetricConstants.BIAS_FRACTION,
                                                                 null );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( first ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", first.equals( second ) );
        assertTrue( "Unexpected inequality between two metadata instances.", second.equals( first ) );
        // Transitive
        MetricOutputMetadata secondT = metaFac.getOutputMetadata( 1,
                                                                  metaFac.getDimension( "CMS" ),
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
        MetricOutputMetadata third = metaFac.getOutputMetadata( 2,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( third ) );
        MetricOutputMetadata fourth = metaFac.getOutputMetadata( 1,
                                                                 metaFac.getDimension( "CFS" ),
                                                                 base,
                                                                 MetricConstants.BIAS_FRACTION,
                                                                 null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( fourth ) );
        MetricOutputMetadata fifth = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                null );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( fifth ) );
        MetricOutputMetadata sixth = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", first.equals( sixth ) );
        // Unequal input dimensions
        Location seventhLocation = metaFac.getLocation( "DRRC3" );
        MetricOutputMetadata seventh = metaFac.getOutputMetadata( 2,
                                                                  metaFac.getDimension( "CMS" ),
                                                                  metaFac.getMetadata( metaFac.getDimension( "OTHER_DIM" ),
                                                                                       metaFac.getDatasetIdentifier( seventhLocation,
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
    public void outputMetadataMinimumEquals()
    {
        Location locationBase = metaFac.getLocation( "DRRC3" );
        Metadata base = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                             metaFac.getDatasetIdentifier( locationBase, "SQIN", "HEFS" ) );
        MetricOutputMetadata first = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                null );
        MetricOutputMetadata second = metaFac.getOutputMetadata( 2,
                                                                 metaFac.getDimension( "CMS" ),
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
        MetricOutputMetadata secondT = metaFac.getOutputMetadata( 1,
                                                                  metaFac.getDimension( "CMS" ),
                                                                  base,
                                                                  MetricConstants.BIAS_FRACTION,
                                                                  null );

        assertTrue( "Unexpected inequality between two metadata instances.", second.minimumEquals( secondT ) );
        assertTrue( "Unexpected inequality between two metadata instances.", first.minimumEquals( secondT ) );
        // Unequal
        MetricOutputMetadata third = metaFac.getOutputMetadata( 2,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                null );
        assertFalse( "Unexpected equality between two metadata instances.", first.minimumEquals( third ) );
        MetricOutputMetadata fourth = metaFac.getOutputMetadata( 2,
                                                                 metaFac.getDimension( "CMS" ),
                                                                 base,
                                                                 MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                 MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", third.minimumEquals( fourth ) );
        MetricOutputMetadata fifth = metaFac.getOutputMetadata( 2,
                                                                metaFac.getDimension( "CFS" ),
                                                                base,
                                                                MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata instances.", fourth.minimumEquals( fifth ) );
        Location secondLocation = metaFac.getLocation( "DRRC3" );
        Metadata baseSecond = metaFac.getMetadata( metaFac.getDimension( "OTHER_DIM" ),
                                                   metaFac.getDatasetIdentifier( secondLocation, "SQIN", "HEFS" ) );

        MetricOutputMetadata sixth = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                baseSecond,
                                                                MetricConstants.BIAS_FRACTION,
                                                                null );
        assertFalse( "Unexpected equality between two metadata instances.", first.minimumEquals( sixth ) );


    }

    /**
     * Test {@link MetricOutputMetadata#hashCode()}.
     */

    @Test
    public void outputMetadataHashCode()
    {
        // Equal
        TimeWindow firstWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Location baseLocation = metaFac.getLocation( "DRRC3" );
        Metadata base = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                             metaFac.getDatasetIdentifier( baseLocation, "SQIN", "HEFS" ),
                                             firstWindow );
        MetricOutputMetadata first = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                null );
        MetricOutputMetadata second = metaFac.getOutputMetadata( 1,
                                                                 metaFac.getDimension( "CMS" ),
                                                                 base,
                                                                 MetricConstants.BIAS_FRACTION,
                                                                 null );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == first.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", first.hashCode() == second.hashCode() );
        MetricOutputMetadata secondT = metaFac.getOutputMetadata( 1,
                                                                  metaFac.getDimension( "CMS" ),
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
        MetricOutputMetadata third = metaFac.getOutputMetadata( 2,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == third.hashCode() );
        MetricOutputMetadata fourth = metaFac.getOutputMetadata( 1,
                                                                 metaFac.getDimension( "CFS" ),
                                                                 base,
                                                                 MetricConstants.BIAS_FRACTION,
                                                                 null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == fourth.hashCode() );
        MetricOutputMetadata fifth = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                null );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == fifth.hashCode() );
        MetricOutputMetadata sixth = metaFac.getOutputMetadata( 1,
                                                                metaFac.getDimension( "CMS" ),
                                                                base,
                                                                MetricConstants.BIAS_FRACTION,
                                                                MetricConstants.NONE );
        assertFalse( "Unexpected equality between two metadata hashcodes.", first.hashCode() == sixth.hashCode() );
        // Unequal input dimensions
        Location seventhLocation = metaFac.getLocation( "DRRC3" );
        MetricOutputMetadata seventh = metaFac.getOutputMetadata( 2,
                                                                  metaFac.getDimension( "CMS" ),
                                                                  metaFac.getMetadata( metaFac.getDimension( "OTHER_DIM" ),
                                                                                       metaFac.getDatasetIdentifier( seventhLocation,
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

    /**
     * Test {@link Dimension#equals(Object)}.
     */

    @Test
    public void dimensionEquals()
    {
        assertTrue( "Unexpected inequality between two dimension instances.",
                    metaFac.getDimension().equals( metaFac.getDimension() ) );
        Dimension m1 = metaFac.getDimension( "A" );
        // Reflexive
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m1 ) );
        Dimension m2 = metaFac.getDimension( "A" );
        // Symmetric
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m1 ) );
        Dimension m3 = metaFac.getDimension( "A" );
        // Transitive
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        }
        // Unequal
        Dimension m4 = metaFac.getDimension( "B" );
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( m4 ) );
        // Null check
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( null ) );
        // Other type check
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link Dimension#hashCode()}.
     */

    @Test
    public void dimensionHashcode()
    {
        // Equal
        assertTrue( "Unexpected inequality between two dimension hashcodes.",
                    metaFac.getDimension().equals( metaFac.getDimension() ) );
        Dimension m1 = metaFac.getDimension( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m1.hashCode() );
        Dimension m2 = metaFac.getDimension( "A" );
        Dimension m3 = metaFac.getDimension( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        Dimension m4 = metaFac.getDimension( "B" );
        assertFalse( "Unexpected equality between two dimension hashcodes.", m1.hashCode() == m4.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two dimension hashcodes.",
                     m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

    /**
     * Test {@link DatasetIdentifier#equals(Object)}.
     */

    @Test
    public void datasetIdentifierEquals()
    {
        // Reflexive
        Location l1 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier m1 = metaFac.getDatasetIdentifier( l1, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m1 ) );

        Location l2 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier m2 = metaFac.getDatasetIdentifier( l2, "SQIN", "HEFS" );

        // Symmetric
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m1 ) );

        // Transitive
        Location l3 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier m3 = metaFac.getDatasetIdentifier( l3, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        }

        // Equal with some identifiers missing
        Location lp1 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p1 = metaFac.getDatasetIdentifier( lp1, "SQIN", null );

        Location lp2 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p2 = metaFac.getDatasetIdentifier( lp2, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p1.equals( p2 ) );

        Location lp3 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p3 = metaFac.getDatasetIdentifier( lp3, null, null );

        Location lp4 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p4 = metaFac.getDatasetIdentifier( lp4, null, null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p3.equals( p4 ) );

        DatasetIdentifier p5 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = metaFac.getDatasetIdentifier( null, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p5.equals( p6 ) );

        // Equal with scenario identifier for baseline
        Location lb1 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier b1 = metaFac.getDatasetIdentifier( lb1, "SQIN", "HEFS", "ESP" );

        Location lb2 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier b2 = metaFac.getDatasetIdentifier( lb2, "SQIN", "HEFS", "ESP" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", b1.equals( b2 ) );

        // Unequal
        Location l4 = metaFac.getLocation( "DRRC3" );
        DatasetIdentifier m4 = metaFac.getDatasetIdentifier( l4, "SQIN", "HEFS" );

        Location l5 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier m5 = metaFac.getDatasetIdentifier( l5, "SQIN2", "HEFS" );

        Location l6 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier m6 = metaFac.getDatasetIdentifier( l6, "SQIN", "HEFS4" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m4 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m5 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m6 ) );

        // Unequal with some identifiers missing
        Location lp7 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p7 = metaFac.getDatasetIdentifier( lp7, "SQIN", null );

        Location lp8 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p8 = metaFac.getDatasetIdentifier( lp8, "SQIN", "HEFS" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p7.equals( p8 ) );

        Location lp9 = metaFac.getLocation( "DRRC2" );
        DatasetIdentifier p9 = metaFac.getDatasetIdentifier( lp9, null, null );

        DatasetIdentifier p10 = metaFac.getDatasetIdentifier( null, null, null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p9.equals( p10 ) );

        DatasetIdentifier p11 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = metaFac.getDatasetIdentifier( null, null, null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p11.equals( p12 ) );

        // Unequal scenario identifiers for baseline
        Location lb3 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b3 = metaFac.getDatasetIdentifier( lb3, "SQIN", "HEFS", "ESP" );

        Location lb4 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b4 = metaFac.getDatasetIdentifier( lb4, "SQIN", "HEFS", "ESP2" );

        Location lb5 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b5 = metaFac.getDatasetIdentifier( lb5, "SQIN", "HEFS", null );

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
    public void datasetIdentifierHashCode()
    {
        // Equal
        Location l1 = metaFac.getLocation("DRRC2");
        DatasetIdentifier m1 = metaFac.getDatasetIdentifier( l1, "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m1.hashCode() );
        Location l2 = metaFac.getLocation("DRRC2");
        DatasetIdentifier m2 = metaFac.getDatasetIdentifier( l2, "SQIN", "HEFS" );
        Location l3 = metaFac.getLocation("DRRC2");
        DatasetIdentifier m3 = metaFac.getDatasetIdentifier( l3, "SQIN", "HEFS" );
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
        Location l4 = metaFac.getLocation("DRRC3");
        DatasetIdentifier m4 = metaFac.getDatasetIdentifier( l4, "SQIN", "HEFS" );
        Location l5 = metaFac.getLocation("DRRC2");
        DatasetIdentifier m5 = metaFac.getDatasetIdentifier( l5, "SQIN2", "HEFS" );
        Location l6 = metaFac.getLocation("DRRC2");
        DatasetIdentifier m6 = metaFac.getDatasetIdentifier( l6, "SQIN", "HEFS4" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m4.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m5.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m6.hashCode() );

        // Equal with some identifiers missing
        Location lp1 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p1 = metaFac.getDatasetIdentifier( lp1, "SQIN", null );
        Location lp2 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p2 = metaFac.getDatasetIdentifier( lp2, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p1.hashCode() == p2.hashCode() );
        Location lp3 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p3 = metaFac.getDatasetIdentifier( lp3, null, null );
        Location lp4 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p4 = metaFac.getDatasetIdentifier( lp4, null, null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p3.hashCode() == p4.hashCode() );
        DatasetIdentifier p5 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p5.hashCode() == p6.hashCode() );
        // Equal with scenario identifier for baseline
        Location lb1 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b1 = metaFac.getDatasetIdentifier( lb1, "SQIN", "HEFS", "ESP" );
        Location lb2 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b2 = metaFac.getDatasetIdentifier( lb2, "SQIN", "HEFS", "ESP" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", b1.hashCode() == b2.hashCode() );
        // Unequal with some identifiers missing
        Location lp7 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p7 = metaFac.getDatasetIdentifier( lp7, "SQIN", null );
        Location lp8 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p8 = metaFac.getDatasetIdentifier( lp8, "SQIN", "HEFS" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p7.hashCode() == p8.hashCode() );
        Location lp9 = metaFac.getLocation("DRRC2");
        DatasetIdentifier p9 = metaFac.getDatasetIdentifier( lp9, null, null );
        DatasetIdentifier p10 = metaFac.getDatasetIdentifier( null, null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p9.hashCode() == p10.hashCode() );
        DatasetIdentifier p11 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = metaFac.getDatasetIdentifier( null, null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     p11.hashCode() == p12.hashCode() );
        // Unequal scenario identifiers for baseline
        Location lb3 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b3 = metaFac.getDatasetIdentifier( lb3, "SQIN", "HEFS", "ESP" );
        Location lb4 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b4 = metaFac.getDatasetIdentifier( lb4, "SQIN", "HEFS", "ESP2" );
        Location lb5 = metaFac.getLocation("DRRC2");
        DatasetIdentifier b5 = metaFac.getDatasetIdentifier( lb5, "SQIN", "HEFS", null );
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

    /**
     * Tests {@link Location#equals(Object)}
     */
    @Test
    public void locationEquals()
    {
        Location all = metaFac.getLocation( 18384141L,
                                            "DRRC2",
                                            -108.06F,
                                            37.6389F,
                                            "09165000" );
        Location all2 = metaFac.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        Location all3 = metaFac.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        Location allDiffVectorID = metaFac.getLocation( 1838141L,
                                                        "DRRC2",
                                                        -108.06F,
                                                        37.6389F,
                                                        "09165000" );
        Location allDiffName = metaFac.getLocation( 18384141L,
                                                    "DRRC3",
                                                    -108.06F,
                                                    37.6389F,
                                                    "09165000" );
        Location allDiffCoordinates = metaFac.getLocation( 18384141L,
                                                           "DRRC2",
                                                           -108.106F,
                                                           37.63829F,
                                                           "09165000" );
        Location allDiffGage = metaFac.getLocation( 18384141L,
                                                    "DRRC2",
                                                    -108.06F,
                                                    37.6389F,
                                                    "0916500" );
        Location allDiff = metaFac.getLocation( 1838425141L,
                                                "DRRC3",
                                                -108.061F,
                                                37.63859F,
                                                "0916500" );

        Location name = metaFac.getLocation( "DRRC2" );
        Location name2 = metaFac.getLocation( "DRRC2" );
        Location name3 = metaFac.getLocation( "DRRC2" );
        Location diffName = metaFac.getLocation( "DRRC5" );

        Location vID = metaFac.getLocation(18384141L);
        Location vID2 = metaFac.getLocation(18384141L);
        Location vID3 = metaFac.getLocation(18384141L);
        Location diffVID = metaFac.getLocation(1834584141L);

        Location latLon = metaFac.getLocation( -108.06F, 37.6389F );
        Location latLon2 = metaFac.getLocation( -108.06F, 37.6389F );
        Location latLon3 = metaFac.getLocation( -108.06F, 37.6389F );
        Location diffLatLon = metaFac.getLocation( -101.06F, 37.6389F );

        Location gage = metaFac.getLocation( null, null, null, null, "09165000" );
        Location gage2 = metaFac.getLocation( null, null, null, null, "09165000" );
        Location gage3 = metaFac.getLocation( null, null, null, null, "09165000" );
        Location diffGage = metaFac.getLocation( null, null, null, null, "0916455000" );

        // Reflexive
        assertTrue( "Unexpected inequality between two location identifier instances",
                    all.equals( all ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    name.equals( name ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    vID.equals( vID ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    gage.equals( gage ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    latLon.equals( latLon ));

        // Symmetric
        assertTrue( "Unexpected inequality between two location identifier instances",
                    all.equals( all2 ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    all2.equals( all ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    name.equals( name2 ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    name2.equals( name ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    vID.equals( vID2 ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    vID2.equals( vID ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    gage2.equals( gage ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    latLon.equals( latLon2 ));
        assertTrue( "Unexpected inequality between two location identifier instances",
                    latLon2.equals( latLon ));

        // Transitive
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    all.equals( all3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    all2.equals( all3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    name.equals( name3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    name2.equals( name3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    vID.equals( vID3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    vID2.equals( vID3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    gage.equals( gage3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    gage2.equals( gage3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    latLon.equals( latLon3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    latLon2.equals( latLon3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.",
                        all.equals( all2 ) );
        }

        // Unequal
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiff ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffName ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffVectorID ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffCoordinates ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffGage ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( name ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( vID ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( latLon ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( gage ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     name.equals( diffName ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     vID.equals( diffVID ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     latLon.equals( diffLatLon ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     gage.equals( diffGage ) );

        // Null check
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( null ) );

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals(Double.valueOf( 2 )) );
    }

    /**
     * Tests {@link Location#hashCode()}
     */
    @Test
    public void locationHashCode()
    {
        // Equal
        Location all = metaFac.getLocation( 18384141L,
                                           "DRRC2",
                                           -108.06F,
                                           37.6389F,
                                           "09165000" );
        Location all2 = metaFac.getLocation( 18384141L,
                                           "DRRC2",
                                           -108.06F,
                                           37.6389F,
                                           "09165000" );
        assertTrue( "Unexpected inequality between two location hashcodes.", all.hashCode() == all2.hashCode() );

        // Consistent
        for (int i = 0; i < 20; i++)
        {
            assertTrue( "Unexpected inequality between two location hashcodes.", all.hashCode() == all2.hashCode() );
        }

        // Equal with Identifiers missing
        Location name = metaFac.getLocation( "DRRC2" );
        Location name2 = metaFac.getLocation( "DRRC2" );
        assertTrue( "Unexpected inequality between two location hashcodes.", name.hashCode() == name2.hashCode() );

        Location vID = metaFac.getLocation(18384141L);
        Location vID2 = metaFac.getLocation(18384141L);
        assertTrue( "Unexpected inequality between two location hashcodes.", vID.hashCode() == vID2.hashCode() );

        Location latLon = metaFac.getLocation( -108.06F, 37.6389F );
        Location latLon2 = metaFac.getLocation( -108.06F, 37.6389F );
        assertTrue( "Unexpected inequality between two location hashcodes.", latLon.hashCode() == latLon2.hashCode() );

        Location gage = metaFac.getLocation( null, null, null, null, "09165000" );
        Location gage2 = metaFac.getLocation( null, null, null, null, "09165000" );
        assertTrue( "Unexpected inequality between two location hashcodes.",
                    gage.hashCode() == gage2.hashCode() );

        // Unequal
        Location allDiffVectorID = metaFac.getLocation( 1838141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffVectorID.hashCode() );

        Location allDiffName = metaFac.getLocation( 18384141L,
                                             "DRRC3",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffName.hashCode() );

        Location allDiffCoordinates = metaFac.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.106F,
                                             37.63829F,
                                             "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffCoordinates.hashCode() );

        Location allDiffGage = metaFac.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "0916500" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffGage.hashCode() );

        Location allDiff = metaFac.getLocation( 1838425141L,
                                             "DRRC3",
                                             -108.061F,
                                             37.63859F,
                                             "0916500" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiff.hashCode() );


        // Unequal with some identifiers missing
        Location diffName = metaFac.getLocation( "DRRC5" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                    name.hashCode() == diffName.hashCode() );

        Location diffVID = metaFac.getLocation(1834584141L);
        assertFalse( "Unexpected equality between two location hashcodes.",
                    vID.hashCode() == diffVID.hashCode() );

        Location diffLatLon = metaFac.getLocation( -101.06F, 37.6389F );
        assertFalse( "Unexpected equality between two location hashcodes.",
                    latLon.hashCode() == diffLatLon.hashCode() );

        Location diffGage = metaFac.getLocation( null, null, null, null, "0916455000" );
        assertFalse("Unexpected equality between two location hashcodes.",
                    gage.hashCode() == diffGage.hashCode());

        // Unequal between partial and full objects
        assertFalse("Unexpected equality between two location hashcodes.",
                    all.hashCode() == gage.hashCode());
        assertFalse("Unexpected equality between two location hashcodes.",
                    all.hashCode() == vID.hashCode());
        assertFalse("Unexpected equality between two location hashcodes.",
                    all.hashCode() == name.hashCode());
        assertFalse("Unexpected equality between two location hashcodes.",
                    all.hashCode() == latLon.hashCode());

        // Other type check
        assertFalse( "Unexpected equality between two location hashcodes",
                     all.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

    /**
     * Tests the {@link MetadataFactory#unionOf(java.util.List)} against a benchmark.
     */
    @Test
    public void unionOf()
    {
        Location l1 = metaFac.getLocation( "DRRC2" );
        Metadata m1 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l1, "SQIN", "HEFS" ),
                                           dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1985-12-31T23:59:59Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location l2 = metaFac.getLocation( "DRRC2" );
        Metadata m2 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l2, "SQIN", "HEFS" ),
                                           dataFac.ofTimeWindow( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-12-31T23:59:59Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location l3 = metaFac.getLocation( "DRRC2" );
        Metadata m3 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( l3, "SQIN", "HEFS" ),
                                           dataFac.ofTimeWindow( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1988-01-01T00:00:00Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location benchmarkLocation = metaFac.getLocation( "DRRC2" );
        Metadata benchmark = metaFac.getMetadata( metaFac.getDimension(),
                                                  metaFac.getDatasetIdentifier( benchmarkLocation, "SQIN", "HEFS" ),
                                                  dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                        Instant.parse( "1988-01-01T00:00:00Z" ),
                                                                        ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Unexpected difference between union of metadata and benchmark.",
                    benchmark.equals( metaFac.unionOf( Arrays.asList( m1, m2, m3 ) ) ) );
        //Checked exception
        try
        {
            Location failLocation = metaFac.getLocation( "DRRC3" );
            Metadata fail = metaFac.getMetadata( metaFac.getDimension(),
                                                 metaFac.getDatasetIdentifier( failLocation, "SQIN", "HEFS" ) );
            metaFac.unionOf( Arrays.asList( m1, m2, m3, fail ) );
            fail( "Expected a checked exception on building the union of metadata for unequal inputs." );
        }
        catch ( MetadataException e )
        {
        }
    }

}

package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Arrays;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Dimension;
import wres.datamodel.metadata.Location;
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
 */
public final class MetadataFactoryTest
{

    /**
     * Test {@link Metadata#equals(Object)}.
     */

    @Test
    public void metadataEquals()
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
    public void metadataHashcode()
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

    /**
     * Test {@link MetricOutputMetadata#equals(Object)}.
     */

    @Test
    public void outputMetadataEquals()
    {
        TimeWindow firstWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Location locationBase = MetadataFactory.getLocation( "DRRC3" );
        Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                             MetadataFactory.getDatasetIdentifier( locationBase, "SQIN", "HEFS" ),
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
    public void outputMetadataMinimumEquals()
    {
        Location locationBase = MetadataFactory.getLocation( "DRRC3" );
        Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                             MetadataFactory.getDatasetIdentifier( locationBase, "SQIN", "HEFS" ) );
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
                                                   MetadataFactory.getDatasetIdentifier( secondLocation, "SQIN", "HEFS" ) );

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
    public void outputMetadataHashCode()
    {
        // Equal
        TimeWindow firstWindow = DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Location baseLocation = MetadataFactory.getLocation( "DRRC3" );
        Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "SOME_DIM" ),
                                             MetadataFactory.getDatasetIdentifier( baseLocation, "SQIN", "HEFS" ),
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

    /**
     * Test {@link Dimension#equals(Object)}.
     */

    @Test
    public void dimensionEquals()
    {
        assertTrue( "Unexpected inequality between two dimension instances.",
                    MetadataFactory.getDimension().equals( MetadataFactory.getDimension() ) );
        Dimension m1 = MetadataFactory.getDimension( "A" );
        // Reflexive
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m1 ) );
        Dimension m2 = MetadataFactory.getDimension( "A" );
        // Symmetric
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m1 ) );
        Dimension m3 = MetadataFactory.getDimension( "A" );
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
        Dimension m4 = MetadataFactory.getDimension( "B" );
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
                    MetadataFactory.getDimension().equals( MetadataFactory.getDimension() ) );
        Dimension m1 = MetadataFactory.getDimension( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m1.hashCode() );
        Dimension m2 = MetadataFactory.getDimension( "A" );
        Dimension m3 = MetadataFactory.getDimension( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        Dimension m4 = MetadataFactory.getDimension( "B" );
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
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m1 = MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m1 ) );

        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m2 = MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" );

        // Symmetric
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m1 ) );

        // Transitive
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m3 = MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        }

        // Equal with some identifiers missing
        Location lp1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p1 = MetadataFactory.getDatasetIdentifier( lp1, "SQIN", null );

        Location lp2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p2 = MetadataFactory.getDatasetIdentifier( lp2, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p1.equals( p2 ) );

        Location lp3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p3 = MetadataFactory.getDatasetIdentifier( lp3, null, null );

        Location lp4 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p4 = MetadataFactory.getDatasetIdentifier( lp4, null, null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p3.equals( p4 ) );

        DatasetIdentifier p5 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p5.equals( p6 ) );

        // Equal with scenario identifier for baseline
        Location lb1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b1 = MetadataFactory.getDatasetIdentifier( lb1, "SQIN", "HEFS", "ESP" );

        Location lb2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b2 = MetadataFactory.getDatasetIdentifier( lb2, "SQIN", "HEFS", "ESP" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", b1.equals( b2 ) );

        // Unequal
        Location l4 = MetadataFactory.getLocation( "DRRC3" );
        DatasetIdentifier m4 = MetadataFactory.getDatasetIdentifier( l4, "SQIN", "HEFS" );

        Location l5 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m5 = MetadataFactory.getDatasetIdentifier( l5, "SQIN2", "HEFS" );

        Location l6 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m6 = MetadataFactory.getDatasetIdentifier( l6, "SQIN", "HEFS4" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m4 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m5 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m6 ) );

        // Unequal with some identifiers missing
        Location lp7 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p7 = MetadataFactory.getDatasetIdentifier( lp7, "SQIN", null );

        Location lp8 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p8 = MetadataFactory.getDatasetIdentifier( lp8, "SQIN", "HEFS" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p7.equals( p8 ) );

        Location lp9 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p9 = MetadataFactory.getDatasetIdentifier( lp9, null, null );

        DatasetIdentifier p10 = MetadataFactory.getDatasetIdentifier( null, "VAR", null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p9.equals( p10 ) );

        DatasetIdentifier p11 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "AB" ), null, null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p11.equals( p12 ) );

        // Unequal scenario identifiers for baseline
        Location lb3 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b3 = MetadataFactory.getDatasetIdentifier( lb3, "SQIN", "HEFS", "ESP" );

        Location lb4 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b4 = MetadataFactory.getDatasetIdentifier( lb4, "SQIN", "HEFS", "ESP2" );

        Location lb5 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b5 = MetadataFactory.getDatasetIdentifier( lb5, "SQIN", "HEFS", null );

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
        Location l1 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier m1 = MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m1.hashCode() );
        Location l2 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier m2 = MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" );
        Location l3 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier m3 = MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" );
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
        Location l4 = MetadataFactory.getLocation("DRRC3");
        DatasetIdentifier m4 = MetadataFactory.getDatasetIdentifier( l4, "SQIN", "HEFS" );
        Location l5 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier m5 = MetadataFactory.getDatasetIdentifier( l5, "SQIN2", "HEFS" );
        Location l6 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier m6 = MetadataFactory.getDatasetIdentifier( l6, "SQIN", "HEFS4" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m4.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m5.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m6.hashCode() );

        // Equal with some identifiers missing
        Location lp1 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p1 = MetadataFactory.getDatasetIdentifier( lp1, "SQIN", null );
        Location lp2 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p2 = MetadataFactory.getDatasetIdentifier( lp2, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p1.hashCode() == p2.hashCode() );
        Location lp3 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p3 = MetadataFactory.getDatasetIdentifier( lp3, null, null );
        Location lp4 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p4 = MetadataFactory.getDatasetIdentifier( lp4, null, null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p3.hashCode() == p4.hashCode() );
        DatasetIdentifier p5 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p5.hashCode() == p6.hashCode() );
        // Equal with scenario identifier for baseline
        Location lb1 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b1 = MetadataFactory.getDatasetIdentifier( lb1, "SQIN", "HEFS", "ESP" );
        Location lb2 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b2 = MetadataFactory.getDatasetIdentifier( lb2, "SQIN", "HEFS", "ESP" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", b1.hashCode() == b2.hashCode() );
        // Unequal with some identifiers missing
        Location lp7 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p7 = MetadataFactory.getDatasetIdentifier( lp7, "SQIN", null );
        Location lp8 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p8 = MetadataFactory.getDatasetIdentifier( lp8, "SQIN", "HEFS" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p7.hashCode() == p8.hashCode() );
        Location lp9 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier p9 = MetadataFactory.getDatasetIdentifier( lp9, null, null );
        DatasetIdentifier p10 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p9.hashCode() == p10.hashCode() );
        DatasetIdentifier p11 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation("LOC"), null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     p11.hashCode() == p12.hashCode() );
        // Unequal scenario identifiers for baseline
        Location lb3 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b3 = MetadataFactory.getDatasetIdentifier( lb3, "SQIN", "HEFS", "ESP" );
        Location lb4 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b4 = MetadataFactory.getDatasetIdentifier( lb4, "SQIN", "HEFS", "ESP2" );
        Location lb5 = MetadataFactory.getLocation("DRRC2");
        DatasetIdentifier b5 = MetadataFactory.getDatasetIdentifier( lb5, "SQIN", "HEFS", null );
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
        Location all = MetadataFactory.getLocation( 18384141L,
                                            "DRRC2",
                                            -108.06F,
                                            37.6389F,
                                            "09165000" );
        Location all2 = MetadataFactory.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        Location all3 = MetadataFactory.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        Location allDiffVectorID = MetadataFactory.getLocation( 1838141L,
                                                        "DRRC2",
                                                        -108.06F,
                                                        37.6389F,
                                                        "09165000" );
        Location allDiffName = MetadataFactory.getLocation( 18384141L,
                                                    "DRRC3",
                                                    -108.06F,
                                                    37.6389F,
                                                    "09165000" );
        Location allDiffCoordinates = MetadataFactory.getLocation( 18384141L,
                                                           "DRRC2",
                                                           -108.106F,
                                                           37.63829F,
                                                           "09165000" );
        Location allDiffGage = MetadataFactory.getLocation( 18384141L,
                                                    "DRRC2",
                                                    -108.06F,
                                                    37.6389F,
                                                    "0916500" );
        Location allDiff = MetadataFactory.getLocation( 1838425141L,
                                                "DRRC3",
                                                -108.061F,
                                                37.63859F,
                                                "0916500" );

        Location name = MetadataFactory.getLocation( "DRRC2" );
        Location name2 = MetadataFactory.getLocation( "DRRC2" );
        Location name3 = MetadataFactory.getLocation( "DRRC2" );
        Location diffName = MetadataFactory.getLocation( "DRRC5" );

        Location vID = MetadataFactory.getLocation(18384141L);
        Location vID2 = MetadataFactory.getLocation(18384141L);
        Location vID3 = MetadataFactory.getLocation(18384141L);
        Location diffVID = MetadataFactory.getLocation(1834584141L);

        Location latLon = MetadataFactory.getLocation( -108.06F, 37.6389F );
        Location latLon2 = MetadataFactory.getLocation( -108.06F, 37.6389F );
        Location latLon3 = MetadataFactory.getLocation( -108.06F, 37.6389F );
        Location diffLatLon = MetadataFactory.getLocation( -101.06F, 37.6389F );

        Location gage = MetadataFactory.getLocation( null, null, null, null, "09165000" );
        Location gage2 = MetadataFactory.getLocation( null, null, null, null, "09165000" );
        Location gage3 = MetadataFactory.getLocation( null, null, null, null, "09165000" );
        Location diffGage = MetadataFactory.getLocation( null, null, null, null, "0916455000" );

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
        Location all = MetadataFactory.getLocation( 18384141L,
                                           "DRRC2",
                                           -108.06F,
                                           37.6389F,
                                           "09165000" );
        Location all2 = MetadataFactory.getLocation( 18384141L,
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
        Location name = MetadataFactory.getLocation( "DRRC2" );
        Location name2 = MetadataFactory.getLocation( "DRRC2" );
        assertTrue( "Unexpected inequality between two location hashcodes.", name.hashCode() == name2.hashCode() );

        Location vID = MetadataFactory.getLocation(18384141L);
        Location vID2 = MetadataFactory.getLocation(18384141L);
        assertTrue( "Unexpected inequality between two location hashcodes.", vID.hashCode() == vID2.hashCode() );

        Location latLon = MetadataFactory.getLocation( -108.06F, 37.6389F );
        Location latLon2 = MetadataFactory.getLocation( -108.06F, 37.6389F );
        assertTrue( "Unexpected inequality between two location hashcodes.", latLon.hashCode() == latLon2.hashCode() );

        Location gage = MetadataFactory.getLocation( null, null, null, null, "09165000" );
        Location gage2 = MetadataFactory.getLocation( null, null, null, null, "09165000" );
        assertTrue( "Unexpected inequality between two location hashcodes.",
                    gage.hashCode() == gage2.hashCode() );

        // Unequal
        Location allDiffVectorID = MetadataFactory.getLocation( 1838141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffVectorID.hashCode() );

        Location allDiffName = MetadataFactory.getLocation( 18384141L,
                                             "DRRC3",
                                             -108.06F,
                                             37.6389F,
                                             "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffName.hashCode() );

        Location allDiffCoordinates = MetadataFactory.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.106F,
                                             37.63829F,
                                             "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffCoordinates.hashCode() );

        Location allDiffGage = MetadataFactory.getLocation( 18384141L,
                                             "DRRC2",
                                             -108.06F,
                                             37.6389F,
                                             "0916500" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffGage.hashCode() );

        Location allDiff = MetadataFactory.getLocation( 1838425141L,
                                             "DRRC3",
                                             -108.061F,
                                             37.63859F,
                                             "0916500" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiff.hashCode() );


        // Unequal with some identifiers missing
        Location diffName = MetadataFactory.getLocation( "DRRC5" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                    name.hashCode() == diffName.hashCode() );

        Location diffVID = MetadataFactory.getLocation(1834584141L);
        assertFalse( "Unexpected equality between two location hashcodes.",
                    vID.hashCode() == diffVID.hashCode() );

        Location diffLatLon = MetadataFactory.getLocation( -101.06F, 37.6389F );
        assertFalse( "Unexpected equality between two location hashcodes.",
                    latLon.hashCode() == diffLatLon.hashCode() );

        Location diffGage = MetadataFactory.getLocation( null, null, null, null, "0916455000" );
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
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m1 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                           MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" ),
                                           DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1985-12-31T23:59:59Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m2 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                           MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" ),
                                           DataFactory.ofTimeWindow( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1986-12-31T23:59:59Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        Metadata m3 = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                           MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" ),
                                           DataFactory.ofTimeWindow( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                 Instant.parse( "1988-01-01T00:00:00Z" ),
                                                                 ReferenceTime.ISSUE_TIME ) );
        Location benchmarkLocation = MetadataFactory.getLocation( "DRRC2" );
        Metadata benchmark = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                  MetadataFactory.getDatasetIdentifier( benchmarkLocation, "SQIN", "HEFS" ),
                                                  DataFactory.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                        Instant.parse( "1988-01-01T00:00:00Z" ),
                                                                        ReferenceTime.ISSUE_TIME ) );
        assertTrue( "Unexpected difference between union of metadata and benchmark.",
                    benchmark.equals( MetadataFactory.unionOf( Arrays.asList( m1, m2, m3 ) ) ) );
        //Checked exception
        try
        {
            Location failLocation = MetadataFactory.getLocation( "DRRC3" );
            Metadata fail = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                 MetadataFactory.getDatasetIdentifier( failLocation, "SQIN", "HEFS" ) );
            MetadataFactory.unionOf( Arrays.asList( m1, m2, m3, fail ) );
            fail( "Expected a checked exception on building the union of metadata for unequal inputs." );
        }
        catch ( MetadataException e )
        {
        }
    }

}

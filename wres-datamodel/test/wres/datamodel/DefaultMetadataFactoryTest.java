package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.metadata.Metadata;
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
        Metadata m1 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        // Reflexive
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m1 ) );
        Metadata m2 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        // Symmetric
        assertTrue( "Unexpected inequality between two metadata instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m2.equals( m1 ) );
        Metadata m3 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        Metadata m4 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m1.equals( m3 ) );
        // Transitive
        Metadata m4t = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                            metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.equals( m4t ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.equals( m4t ) );
        // Unequal
        Metadata m5 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ) );
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
        Metadata m6 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
                                           firstWindow );
        Metadata m7 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
                                           secondWindow );
        assertTrue( "Unexpected inequality between two metadata instances.", m6.equals( m7 ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m7.equals( m6 ) );
        assertFalse( "Unexpected equality between two metadata instances.", m3.equals( m6 ) );
        TimeWindow thirdWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Metadata m8 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
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
        Metadata m1 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m1.hashCode() == m1.hashCode() );
        Metadata m2 = metaFac.getMetadata( metaFac.getDimension(),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m1.hashCode() == m2.hashCode() );
        Metadata m3 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        Metadata m4 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m3.hashCode() == m4.hashCode() );
        Metadata m4t = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                            metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        assertTrue( "Unexpected inequality between two metadata instances.", m4.hashCode() == m4t.hashCode() );
        assertTrue( "Unexpected inequality between two metadata instances.", m3.hashCode() == m4t.hashCode() );
        // Unequal        
        assertFalse( "Unexpected equality between two metadata hashcodes.", m1.hashCode() == m3.hashCode() );
        Metadata m5 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ) );
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
        Metadata m6 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
                                           firstWindow );
        Metadata m7 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
                                           secondWindow );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m6.hashCode() == m7.hashCode() );
        assertTrue( "Unexpected inequality between two metadata hashcodes.", m7.hashCode() == m6.hashCode() );
        assertFalse( "Unexpected equality between two metadata hashcodes.", m3.hashCode() == m6.hashCode() );
        TimeWindow thirdWindow = dataFac.ofTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                       Instant.parse( "1986-01-01T00:00:00Z" ),
                                                       ReferenceTime.ISSUE_TIME );
        Metadata m8 = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                           metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
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
        Metadata base = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                             metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
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
        MetricOutputMetadata seventh = metaFac.getOutputMetadata( 2,
                                                                  metaFac.getDimension( "CMS" ),
                                                                  metaFac.getMetadata( metaFac.getDimension( "OTHER_DIM" ),
                                                                                       metaFac.getDatasetIdentifier( "DRRC3",
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
        Metadata base = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                             metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ) );
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
        Metadata baseSecond = metaFac.getMetadata( metaFac.getDimension( "OTHER_DIM" ),
                                             metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ) );
        
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
        Metadata base = metaFac.getMetadata( metaFac.getDimension( "SOME_DIM" ),
                                             metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" ),
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
        MetricOutputMetadata seventh = metaFac.getOutputMetadata( 2,
                                                                  metaFac.getDimension( "CMS" ),
                                                                  metaFac.getMetadata( metaFac.getDimension( "OTHER_DIM" ),
                                                                                       metaFac.getDatasetIdentifier( "DRRC3",
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
        DatasetIdentifier m1 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m1 ) );
        DatasetIdentifier m2 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        // Symmetric
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m1 ) );
        // Transitive
        DatasetIdentifier m3 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        }
        // Equal with some identifiers missing
        DatasetIdentifier p1 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", null );
        DatasetIdentifier p2 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", p1.equals( p2 ) );
        DatasetIdentifier p3 = metaFac.getDatasetIdentifier( "DRRC2", null, null );
        DatasetIdentifier p4 = metaFac.getDatasetIdentifier( "DRRC2", null, null );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", p3.equals( p4 ) );
        DatasetIdentifier p5 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", p5.equals( p6 ) );      
        // Equal with scenario identifier for baseline
        DatasetIdentifier b1 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP" );
        DatasetIdentifier b2 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP" );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", b1.equals( b2 ) );       
        // Unequal
        DatasetIdentifier m4 = metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" );
        DatasetIdentifier m5 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN2", "HEFS" );
        DatasetIdentifier m6 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS4" );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m4 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m5 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m6 ) );
        // Unequal with some identifiers missing
        DatasetIdentifier p7 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", null );
        DatasetIdentifier p8 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        assertFalse( "Unexpected equality between two dataset identifier instances.", p7.equals( p8 ) );
        DatasetIdentifier p9 = metaFac.getDatasetIdentifier( "DRRC2", null, null );
        DatasetIdentifier p10 = metaFac.getDatasetIdentifier( null, null, null );
        assertFalse( "Unexpected equality between two dataset identifier instances.", p9.equals( p10 ) );
        DatasetIdentifier p11 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = metaFac.getDatasetIdentifier( null, null, null );
        assertFalse( "Unexpected equality between two dataset identifier instances.", p11.equals( p12 ) );

        // Unequal scenario identifiers for baseline
        DatasetIdentifier b3 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP" );
        DatasetIdentifier b4 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP2" );
        DatasetIdentifier b5 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", null );
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
        DatasetIdentifier m1 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m1.hashCode() );
        DatasetIdentifier m2 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        DatasetIdentifier m3 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
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
        DatasetIdentifier m4 = metaFac.getDatasetIdentifier( "DRRC3", "SQIN", "HEFS" );
        DatasetIdentifier m5 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN2", "HEFS" );
        DatasetIdentifier m6 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS4" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m4.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m5.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m6.hashCode() );
               
        // Equal with some identifiers missing
        DatasetIdentifier p1 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", null );
        DatasetIdentifier p2 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p1.hashCode() == p2.hashCode() );
        DatasetIdentifier p3 = metaFac.getDatasetIdentifier( "DRRC2", null, null );
        DatasetIdentifier p4 = metaFac.getDatasetIdentifier( "DRRC2", null, null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p3.hashCode() == p4.hashCode() );
        DatasetIdentifier p5 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p5.hashCode() == p6.hashCode() );      
        // Equal with scenario identifier for baseline
        DatasetIdentifier b1 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP" );
        DatasetIdentifier b2 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", b1.hashCode() == b2.hashCode() );       
        // Unequal with some identifiers missing
        DatasetIdentifier p7 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", null );
        DatasetIdentifier p8 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p7.hashCode() == p8.hashCode() );
        DatasetIdentifier p9 = metaFac.getDatasetIdentifier( "DRRC2", null, null );
        DatasetIdentifier p10 = metaFac.getDatasetIdentifier( null, null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p9.hashCode() == p10.hashCode() );
        DatasetIdentifier p11 = metaFac.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = metaFac.getDatasetIdentifier( null, null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p11.hashCode() == p12.hashCode() );
        // Unequal scenario identifiers for baseline
        DatasetIdentifier b3 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP" );
        DatasetIdentifier b4 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", "ESP2" );
        DatasetIdentifier b5 = metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS", null );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.", b3.hashCode() == b4.hashCode() );        
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.", p1.hashCode() == b3.hashCode() );  
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.", b3.hashCode() == b5.hashCode() );        

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }


}

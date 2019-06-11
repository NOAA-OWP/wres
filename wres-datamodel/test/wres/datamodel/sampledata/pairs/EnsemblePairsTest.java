package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.EnsemblePairs.EnsemblePairsBuilder;

/**
 * Tests the {@link EnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class EnsemblePairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Test metadata
     */

    final SampleMetadata meta = SampleMetadata.of();

    /**
     * Tests the {@link EnsemblePairs}.
     */

    @Test
    public void testEnsemblePairs()
    {
        final List<EnsemblePair> values = new ArrayList<>();
        final EnsemblePairsBuilder b = new EnsemblePairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3, 4 } ) );
        }

        EnsemblePairs p = (EnsemblePairs) b.addData( values ).setMetadata( meta ).build();

        //Check baseline
        assertFalse( p.hasBaseline() );
        p = (EnsemblePairs) b.addDataForBaseline( values ).setMetadataForBaseline( meta ).build(); //Add another
        
        //Check that a returned dataset contains the expected number of pairs
        assertTrue( p.getRawData().size() == 10 );
       
        //Check the baseline
        assertTrue( p.hasBaseline() );
        
        //Check the metadata
        b.setMetadata( this.meta );
        p = b.build();
        assertEquals( this.meta, p.getMetadata() );

    }

    /**
     * Tests for an expected exception on construction with a null pair.
     */

    @Test
    public void testExceptionOnConstructionWithNullPair()
    {
        List<EnsemblePair> values = Collections.singletonList( null );
        EnsemblePairsBuilder c = new EnsemblePairsBuilder();
        
        exception.expect( SampleDataException.class );
        c.addData( values ).setMetadata( this.meta ).build();

    }

    /**
     * Tests construction with a climatological value of {@link Double#NaN}.
     */

    @Test
    public void testExceptionOnConstructionWithNaNClimatology()
    {
        EnsemblePair pair = EnsemblePair.of( 1, new double[] { 1 } );
        VectorOfDoubles climatology = VectorOfDoubles.of( Double.NaN );
        EnsemblePairsBuilder c = new EnsemblePairsBuilder();

        exception.expect( SampleDataException.class );
        c.addData( pair ).setMetadata( this.meta ).setClimatology( climatology ).build();
    }
    
}

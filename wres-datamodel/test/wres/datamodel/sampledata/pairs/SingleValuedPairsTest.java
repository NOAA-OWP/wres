package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.SingleValuedPairs.SingleValuedPairsBuilder;

/**
 * Tests the {@link SingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SingleValuedPairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Tests the {@link SingleValuedPairs}.
     */

    @Test
    public void testSingleValuedPairsWithOnePair()
    {
        final List<SingleValuedPair> values = Arrays.asList( SingleValuedPair.of( 1, 1 ) );
        final SingleValuedPairsBuilder b = new SingleValuedPairsBuilder();

        final SampleMetadata meta = SampleMetadata.of();
        SingleValuedPairs p = (SingleValuedPairs) b.addData( values ).setMetadata( meta ).build();

        //Check dataset count
        assertFalse( "Expected a dataset without a baseline [false," + p.hasBaseline() + "].", p.hasBaseline() );
        p = (SingleValuedPairs) b.addDataForBaseline( values ).setMetadataForBaseline( meta ).build(); //Add another

        //Check that a returned dataset contains the expected number of pairs
        assertTrue( "Expected a main dataset with 1 pairs [1," + p.getRawData().size() + "].",
                    p.getRawData().size() == 1 );
    }


    /**
     * Tests the {@link SingleValuedPairs}.
     */

    @Test
    public void testSingleValuedPairsWithTenPairs()
    {
        final List<SingleValuedPair> values = new ArrayList<>();
        final SingleValuedPairsBuilder b = new SingleValuedPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( SingleValuedPair.of( 1, 1 ) );
        }
        final SampleMetadata meta = SampleMetadata.of();
        SingleValuedPairs p = (SingleValuedPairs) b.addData( values ).setMetadata( meta ).build();

        //Check dataset count
        assertFalse( "Expected a dataset without a baseline [false," + p.hasBaseline() + "].", p.hasBaseline() );
        p = (SingleValuedPairs) b.addDataForBaseline( values ).setMetadataForBaseline( meta ).build(); //Add another
        //Check that a returned dataset contains the expected number of pairs
        assertTrue( "Expected a main dataset with ten pairs [10," + p.getRawData().size() + "].",
                    p.getRawData().size() == 10 );
        //Check the baseline
        assertTrue( "Expected a baseline [true," + p.hasBaseline() + "].", p.hasBaseline() );
        //Check the metadata
        b.setMetadata( meta );
        p = b.build();
        assertTrue( "Expected equal metadata.", p.getMetadata().equals( meta ) );
    }

    @Test
    public void testExceptionOnConstructionWithNullPair()
    {
        List<SingleValuedPair> values = Arrays.asList( (SingleValuedPair) null );
        SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
        
        exception.expect( SampleDataException.class );
        
        c.addData( values ).setMetadata( SampleMetadata.of() ).build();
    }

    @Test
    public void testExceptionOnConstructionWithNaNInClimatology()
    {
        List<SingleValuedPair> values = Collections.singletonList( SingleValuedPair.of( 1, 1 ) );
        VectorOfDoubles climatology = VectorOfDoubles.of( Double.NaN );
        SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
        
        exception.expect( SampleDataException.class );
        
        c.addData( values ).setMetadata( SampleMetadata.of() ).setClimatology( climatology ).build();
    }

}

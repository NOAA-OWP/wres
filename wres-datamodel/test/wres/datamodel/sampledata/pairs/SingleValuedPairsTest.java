package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs.SingleValuedPairsBuilder;

/**
 * Tests the {@link SingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SingleValuedPairsTest
{

    /**
     * Tests the {@link SingleValuedPairs}.
     */

    @Test
    public void testSingleValuedPairs()
    {
        final List<SingleValuedPair> values = new ArrayList<>();
        final SingleValuedPairsBuilder b = new SingleValuedPairsBuilder();

        final SampleMetadata meta = SampleMetadata.of();
        SingleValuedPairs p = (SingleValuedPairs) b.addData( values ).setMetadata( meta ).build();

        //Check dataset count
        assertFalse( "Expected a dataset without a baseline [false," + p.hasBaseline() + "].", p.hasBaseline() );
        p = (SingleValuedPairs) b.addDataForBaseline( values ).setMetadataForBaseline( meta ).build(); //Add another
        //Check that a returned dataset contains the expected number of pairs
        assertTrue( "Expected a main dataset with 0 pairs [0," + p.getRawData().size() + "].", p.getRawData().size() == 0 );
    }
    
    
    /**
     * Tests the {@link SingleValuedPairs}.
     */

    @Test
    public void test1SingleValuedPairs()
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

        //Test the exceptions
        //Null pair
        try
        {
            values.clear();
            values.add( null );
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData( values ).setMetadata( meta ).build();
            fail( "Expected a checked exception on invalid inputs: null pair." );
        }
        catch ( final Exception e )
        {
        }

        //Only non-finite climatology
        try
        {
            values.clear();
            values.add( SingleValuedPair.of( 1, 1 ) );
            VectorOfDoubles climatology = VectorOfDoubles.of( new double[] { Double.NaN } );
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData( values ).setMetadata( meta ).setClimatology( climatology ).build();
            fail( "Expected a checked exception on invalid inputs: all climatology data missing." );
        }
        catch ( final SampleDataException e )
        {
        }

    }

}

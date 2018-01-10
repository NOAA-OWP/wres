package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.SafeSingleValuedPairs.SingleValuedPairsBuilder;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link SafeSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeSingleValuedPairsTest
{

    /**
     * Tests the {@link SafeSingleValuedPairs}.
     */

    @Test
    public void test1SingleValuedPairs()
    {
        final List<PairOfDoubles> values = new ArrayList<>();
        final SingleValuedPairsBuilder b = new SingleValuedPairsBuilder();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = metIn.getMetadataFactory();

        for(int i = 0; i < 10; i++)
        {
            values.add(metIn.pairOf(1, 1));
        }
        final Metadata meta = metaFac.getMetadata();
        SingleValuedPairs p = (SingleValuedPairs)b.addData(values).setMetadata(meta).build();

        //Check dataset count
        assertTrue("Expected a dataset without a baseline [false," + p.hasBaseline() + "].", !p.hasBaseline());
        p = (SingleValuedPairs)b.addDataForBaseline(values).setMetadataForBaseline(meta).build(); //Add another
        //Check that a returned dataset contains the expected number of pairs
        assertTrue("Expected a main dataset with ten pairs [10," + p.getData().size() + "].",
                   p.getData().size() == 10);
        //Check the baseline
        assertTrue("Expected a baseline [true," + p.hasBaseline() + "].", p.hasBaseline());
        //Check the metadata
        b.setMetadata(meta);
        p = b.build();
        assertTrue("Expected equal metadata.", p.getMetadata().equals(meta));

        //Test the exceptions
        //Null pair
        try
        {
            values.clear();
            values.add(null);
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final Exception e)
        {
        }
        //Null pair list
        try
        {
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData(null).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair list.");
        }
        catch(final Exception e)
        {
        }
        
        //Only non-finite main pairs
        try
        {
            values.clear();
            values.add( metIn.pairOf( Double.NaN, 1 ) );
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData( values ).setMetadata( meta ).build();
            fail( "Expected a checked exception on invalid inputs: all data missing." );
        }
        catch ( final InsufficientDataException e )
        {
        }
        //Only non-finite baseline pairs
        try
        {
            values.clear();
            values.add( metIn.pairOf( 1, 1 ) );
            final List<PairOfDoubles> baselineValues = new ArrayList<>();
            baselineValues.add( metIn.pairOf( Double.NaN, 1 ) );
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData( values )
             .setMetadata( meta )
             .addDataForBaseline( baselineValues )
             .setMetadataForBaseline( meta )
             .build();
            fail( "Expected a checked exception on invalid inputs: all baseline data missing." );
        }
        catch ( final InsufficientDataException e )
        {
        }
        //Only non-finite climatology
        try
        {
            values.clear();
            values.add( metIn.pairOf( 1, 1 ) );
            VectorOfDoubles climatology = metIn.vectorOf( new double[] { Double.NaN } );
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.addData( values ).setMetadata( meta ).setClimatology( climatology ).build();
            fail( "Expected a checked exception on invalid inputs: all climatology data missing." );
        }
        catch ( final InsufficientDataException e )
        {
        }

    }

}

package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.SafeEnsemblePairs.EnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link SafeEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeEnsemblePairsTest
{

    /**
     * Tests the {@link SafeEnsemblePairs}.
     */

    @Test
    public void test1EnsemblePairs()
    {
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        final EnsemblePairsBuilder b = new EnsemblePairsBuilder();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = metIn.getMetadataFactory();

        for(int i = 0; i < 10; i++)
        {
            values.add(metIn.pairOf(1, new double[]{1,2,3,4}));
        }
        final Metadata meta = metaFac.getMetadata();
        EnsemblePairs p = (EnsemblePairs)b.addData(values).setMetadata(meta).build();

        //Check dataset count
        assertTrue("Expected a dataset without a baseline [false," + p.hasBaseline() + "].", !p.hasBaseline());
        p = (EnsemblePairs)b.addDataForBaseline(values).setMetadataForBaseline(meta).build(); //Add another
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
            final EnsemblePairsBuilder c = new EnsemblePairsBuilder();
            c.addData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final Exception e)
        {
        }
        //Null pair list
        try
        {
            final EnsemblePairsBuilder c = new EnsemblePairsBuilder();
            c.addData(null).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair list.");
        }
        catch(final Exception e)
        {
        }

    }

}

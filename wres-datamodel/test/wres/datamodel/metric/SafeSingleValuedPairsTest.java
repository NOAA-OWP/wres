package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.SafeSingleValuedPairs.SingleValuedPairsBuilder;

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
        final Metadata meta = metaFac.getMetadata(values.size());
        SingleValuedPairs p = b.setData(values).setMetadata(meta).build();

        //Check dataset count
        assertTrue("Expected a dataset without a baseline [false," + p.hasBaseline() + "].", !p.hasBaseline());
        p = b.setDataForBaseline(values).setMetadataForBaseline(meta).build(); //Add another
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
            c.setData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final Exception e)
        {
        }
        //Null pair list
        try
        {
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.setData(null).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair list.");
        }
        catch(final Exception e)
        {
        }

    }

}

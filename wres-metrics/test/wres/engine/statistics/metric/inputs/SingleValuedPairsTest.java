package wres.engine.statistics.metric.inputs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.engine.statistics.metric.inputs.SingleValuedPairs.SingleValuedPairsBuilder;

/**
 * Tests the {@link SingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SingleValuedPairsTest
{

    /**
     * Tests the {@link SingleValuedPairs}.
     */

    @Test
    public void test1SingleValuedPairs()
    {
        final DataFactory d = DataFactory.instance();
        final List<PairOfDoubles> values = new ArrayList<>();

        final SingleValuedPairsBuilder b = new SingleValuedPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(d.pairOf(1, 1));
        }
        final Metadata meta = MetadataFactory.getMetadata(values.size());
        SingleValuedPairs p = b.add(values).setMetadata(meta).build();
        
        //Check dataset count
        assertTrue("Expected dataset count of one [false," + p.hasBaselineForSkill() + "].", !p.hasBaselineForSkill()); 
        p = b.add(values).build(); //Add another
        //Check that a returned dataset is not null
        assertTrue("Expected a dataset with ten pairs in the first index [10,"+p.getData(0).size()+"].",p.getData(0).size()==10); 
        //Check the addition of a Dimension 
        assertTrue("Expected dataset count of two [true," + p.hasBaselineForSkill() + "].", p.hasBaselineForSkill());        
        b.setMetadata(meta);
        p = b.build();
        assertTrue("Expected equal metadata.", p.getMetadata().equals(meta) );           
        
        //Test the exceptions
        //Null pair
        try
        {
            values.clear();
            values.add(null);
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.add(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final Exception e)
        {
        }   
        //Null pair list
        try
        {
            final SingleValuedPairsBuilder c = new SingleValuedPairsBuilder();
            c.add(null).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair list.");
        }
        catch(final Exception e)
        {
        }        
        
    }

}

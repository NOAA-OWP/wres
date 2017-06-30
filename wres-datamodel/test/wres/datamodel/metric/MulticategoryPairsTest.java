package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.MulticategoryPairs.MulticategoryPairsBuilder;

/**
 * Tests the {@link MulticategoryPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MulticategoryPairsTest
{

    /**
     * Tests the {@link MulticategoryPairs}.
     */

    @Test
    public void test1MulticategoryPairs()
    {
        final List<VectorOfBooleans> values = new ArrayList<>();

        final MulticategoryPairsBuilder b = new MulticategoryPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(DataFactory.vectorOf(new boolean[]{true, true}));
        }        
        final Metadata meta = MetadataFactory.getMetadata(values.size(),
                                                          MetadataFactory.getDimension(),
                                                          "Main");         
        MulticategoryPairs p = b.setData(values).setMetadata(meta).build();

        //Check category count
        assertTrue("Unexpected category count on inputs [2," + p.getCategoryCount() + "].", p.getCategoryCount() == 2);
        //Check pair count
        assertTrue("Unexpected pair count at first index [10," + p.getData().size() + "].", p.getData().size() == 10); 
        //Check category count of two when fully expanded
        final MulticategoryPairsBuilder bn = new MulticategoryPairsBuilder();
        values.clear();
        values.add(DataFactory.vectorOf(new boolean[]{true, false, true, false}));
        final MulticategoryPairs q = bn.setData(values).setMetadata(meta).build();
        assertTrue("Unexpected category count on inputs [2," + q.getCategoryCount() + "].", q.getCategoryCount() == 2);
        //Check dataset count
        assertTrue("Expected dataset count of one [false," + p.hasBaseline() + "].", !p.hasBaseline());
        //Check dataset count of two
        p = b.setDataForBaseline(values).setMetadataForBaseline(meta).build(); //Add another
        //Check the addition of a Dimension 
        assertTrue("Expected dataset count of two [true," + p.hasBaseline() + "].", p.hasBaseline());        
        final Metadata t = MetadataFactory.getMetadata(10);
        b.setMetadata(t);
        p= b.build();
        assertTrue("Expected non-null metadata.", p.getMetadata().equals(t) );  
        
        //Check the exceptions
        //Duplicate prediction
        try
        {
            values.clear();
            values.add(DataFactory.vectorOf(new boolean[]{true, false, false, true, false, true}));
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            c.setData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: duplicate predicted outcome.");
        }
        catch(final Exception e)
        {
        }
        //Duplicate observation
        try
        {
            values.clear();
            values.add(DataFactory.vectorOf(new boolean[]{true, true, false, true, false, false}));
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            c.setData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: duplicate observed outcome.");
        }
        catch(final Exception e)
        {
        } 
        //Null pair
        try
        {
            values.clear();
            values.add(null);
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            c.setData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair.");
        }
        catch(final Exception e)
        {
        }
        //Inputs with varying number of categories across pairs
        try
        { 
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            values.clear();
            values.add(DataFactory.vectorOf(new boolean[]{true, false, false, true, false, false}));
            values.add(DataFactory.vectorOf(new boolean[]{true, false, false, false, true, false, false, false}));
            c.setData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: one or more pairs with a varying number of "
                + "categories.");
        }
        catch(final Exception e)
        {
        }
        //Inputs where the observations have more categories
        try
        {
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            values.clear();
            values.add(DataFactory.vectorOf(new boolean[]{true, false, false, false, true, false, false}));
            c.setData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: observations and predictions have "
                + "different numbers of categories.");
        }
        catch(final Exception e)
        {
        }

        //Null pair list
        try
        {
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            c.setData(null).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: null pair list.");
        }
        catch(final Exception e)
        {
        }
 
        //No exception
        try
        {
            values.clear();
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            values.add(DataFactory.vectorOf(new boolean[]{true, false, true, false}));
            c.setData(values).setMetadata(meta).build();
        }
        catch(final Exception e)
        {
            fail("Unexpected exception on valid inputs.");
        }

    }

}

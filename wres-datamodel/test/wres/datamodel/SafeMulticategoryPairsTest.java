package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.VectorOfBooleans;
import wres.datamodel.SafeMulticategoryPairs.MulticategoryPairsBuilder;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link SafeMulticategoryPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMulticategoryPairsTest
{

    /**
     * Tests the {@link SafeMulticategoryPairs}.
     */

    @Test
    public void test1MulticategoryPairs()
    {
        final List<VectorOfBooleans> values = new ArrayList<>();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = metIn.getMetadataFactory();
        final MulticategoryPairsBuilder b = new MulticategoryPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{true, true}));
        }
        final Location l1 = metaFac.getLocation( "DRRC2" );
        final Metadata meta = metaFac.getMetadata(metaFac.getDimension(),
                                                  metaFac.getDatasetIdentifier(l1, "SQIN", "HEFS"));
        MulticategoryPairs p = (MulticategoryPairs)b.addData(values).setMetadata(meta).build();

        //Check category count
        assertTrue("Unexpected category count on inputs [2," + p.getCategoryCount() + "].", p.getCategoryCount() == 2);
        //Check pair count
        assertTrue("Unexpected pair count for main pairs [10," + p.getRawData().size() + "].", p.getRawData().size() == 10);
        //Check category count of two when fully expanded
        final MulticategoryPairsBuilder bn = new MulticategoryPairsBuilder();
        values.clear();
        values.add(metIn.vectorOf(new boolean[]{true, false, true, false}));
        final MulticategoryPairs q = (MulticategoryPairs)bn.addData(values).setMetadata(meta).build();
        assertTrue("Unexpected category count on inputs [2," + q.getCategoryCount() + "].", q.getCategoryCount() == 2);
        //Check for no baseline
        assertTrue("Expected a dataset without a baseline [false," + p.hasBaseline() + "].", !p.hasBaseline());
        //Check for baseline
        p = (MulticategoryPairs)b.addDataForBaseline(values).setMetadataForBaseline(meta).build(); //Add another
        assertTrue("Expected a dataset with a baseline [true," + p.hasBaseline() + "].", p.hasBaseline());
        //Check the metadata
        final Metadata t = metaFac.getMetadata();
        b.setMetadata(t);
        p = b.build();
        assertTrue("Expected non-null metadata.", p.getMetadata().equals(t));

        //Check the exceptions
        //Duplicate prediction
        try
        {
            values.clear();
            values.add(metIn.vectorOf(new boolean[]{true, false, false, true, false, true}));
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            c.addData(values).setMetadata(meta).build();
            fail("Expected a checked exception on invalid inputs: duplicate predicted outcome.");
        }
        catch(final Exception e)
        {
        }
        //Duplicate observation
        try
        {
            values.clear();
            values.add(metIn.vectorOf(new boolean[]{true, true, false, true, false, false}));
            final MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();
            c.addData(values).setMetadata(meta).build();
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
            c.addData(values).setMetadata(meta).build();
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
            values.add(metIn.vectorOf(new boolean[]{true, false, false, true, false, false}));
            values.add(metIn.vectorOf(new boolean[]{true, false, false, false, true, false, false, false}));
            c.addData(values).setMetadata(meta).build();
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
            values.add(metIn.vectorOf(new boolean[]{true, false, false, false, true, false, false}));
            c.addData(values).setMetadata(meta).build();
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
            c.addData(null).setMetadata(meta).build();
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
            values.add(metIn.vectorOf(new boolean[]{true, false, true, false}));
            c.addData(values).setMetadata(meta).build();
        }
        catch(final Exception e)
        {
            fail("Unexpected exception on valid inputs.");
        }

    }

}

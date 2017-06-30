package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.DichotomousPairs.DichotomousPairsBuilder;

/**
 * Tests the {@link DichotomousPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DichotomousPairsTest
{

    /**
     * Tests the {@link DichotomousPairs}.
     */

    @Test
    public void test1DichotomousPairs()
    {
        final List<VectorOfBooleans> values = new ArrayList<>();

        final DichotomousPairsBuilder b = new DichotomousPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(DataFactory.vectorOf(new boolean[]{true, true}));
        }
        
        final Metadata meta = MetadataFactory.getMetadata(values.size(),
                                                          MetadataFactory.getDimension(),
                                                          "Main");  
        
        final DichotomousPairs p = (DichotomousPairs)b.setData(values).setMetadata(meta).build();

        //Check category count
        assertTrue("Unexpected category count on inputs [2," + p.getCategoryCount() + "].", p.getCategoryCount() == 2);

        //Check the exceptions 
        //Too many categories
        try
        {
            values.clear();
            values.add(DataFactory.vectorOf(new boolean[]{true, false, false, true, false, false}));
            b.setData(values).build();
            fail("Expected a checked exception on invalid inputs.");
        }
        catch(final Exception e)
        {

        }
        //Valid data
        try
        {
            values.clear();
            values.add(DataFactory.vectorOf(new boolean[]{true, false, true, false}));
            b.setData(values).build();
        }
        catch(final Exception e)
        {
            fail("Unexpected exception on valid inputs.");
        }

    }

}

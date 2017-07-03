package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder;

/**
 * Tests the {@link DiscreteProbabilityPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DiscreteProbabilityPairsTest
{

    /**
     * Tests the {@link DiscreteProbabilityPairs}.
     */

    @Test
    public void test1DiscreteProbabilityPairs()
    {
        final List<PairOfDoubles> values = new ArrayList<>();

        final DiscreteProbabilityPairsBuilder b = new DiscreteProbabilityPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(DataFactory.pairOf(1, 1));
        }       
        final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        final Metadata meta = metaFac.getMetadata(values.size(),
                                                          metaFac.getDimension(),
                                                          "Main");         
        b.setData(values).setMetadata(meta).build();
    }

}

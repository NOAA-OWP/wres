package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder;

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
        final DataFactory d = DataFactory.instance();
        final List<PairOfDoubles> values = new ArrayList<>();

        final DiscreteProbabilityPairsBuilder b = new DiscreteProbabilityPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(d.pairOf(1, 1));
        }       
        final Metadata meta = MetadataFactory.getMetadata(values.size(),
                                                          MetadataFactory.getDimension(),
                                                          "Main");         
        b.setData(values).setMetadata(meta).build();
    }

}

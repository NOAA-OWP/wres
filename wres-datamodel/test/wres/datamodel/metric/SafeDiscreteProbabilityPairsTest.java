package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder;

/**
 * Tests the {@link SafeDiscreteProbabilityPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeDiscreteProbabilityPairsTest
{

    /**
     * Tests the {@link SafeDiscreteProbabilityPairs}.
     */

    @Test
    public void test1DiscreteProbabilityPairs()
    {
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory d = DefaultDataFactory.getInstance();
        final DiscreteProbabilityPairsBuilder b = new DiscreteProbabilityPairsBuilder();

        for(int i = 0; i < 10; i++)
        {
            values.add(d.pairOf(1, 1));
        }
        final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        final Metadata meta = metaFac.getMetadata(metaFac.getDimension(),
                                                  metaFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        b.setData(values).setMetadata(meta).build();
    }

}

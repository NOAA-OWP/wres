package wres.datamodel;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

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
        final Location location = metaFac.getLocation( "DRRC2" );
        final Metadata meta = metaFac.getMetadata(metaFac.getDimension(),
                                                  metaFac.getDatasetIdentifier(location, "SQIN", "HEFS"));
        b.addData(values).setMetadata(meta).build();
    }

}

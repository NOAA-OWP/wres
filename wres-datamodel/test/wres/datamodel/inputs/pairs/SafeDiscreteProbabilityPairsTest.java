package wres.datamodel.inputs.pairs;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SafeDiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder;
import wres.datamodel.metadata.Location;
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
        final DiscreteProbabilityPairsBuilder b = new DiscreteProbabilityPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( DataFactory.pairOf( 1, 1 ) );
        }

        final Location location = MetadataFactory.getLocation( "DRRC2" );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( location,
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        b.addData( values ).setMetadata( meta ).build();
    }

}

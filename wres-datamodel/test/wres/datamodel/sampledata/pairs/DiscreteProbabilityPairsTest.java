package wres.datamodel.sampledata.pairs;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPair;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder;

/**
 * Tests the {@link DiscreteProbabilityPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DiscreteProbabilityPairsTest
{

    /**
     * Tests the {@link DiscreteProbabilityPairs}.
     */

    @Test
    public void test1DiscreteProbabilityPairs()
    {
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        final DiscreteProbabilityPairsBuilder b = new DiscreteProbabilityPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        }

        final Location location = Location.of( "DRRC2" );
        final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                           DatasetIdentifier.of( location,
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        b.addData( values ).setMetadata( meta ).build();
    }

}

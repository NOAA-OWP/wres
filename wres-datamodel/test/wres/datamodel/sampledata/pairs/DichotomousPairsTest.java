package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.sampledata.pairs.DichotomousPair;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.MulticategoryPair;
import wres.datamodel.sampledata.pairs.DichotomousPairs.DichotomousPairsBuilder;

/**
 * Tests the {@link DichotomousPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DichotomousPairsTest
{

    /**
     * Tests the {@link DichotomousPairs}.
     */

    @Test
    public void test1DichotomousPairs()
    {
        final List<DichotomousPair> values = new ArrayList<>();

        final DichotomousPairsBuilder b = new DichotomousPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( DichotomousPair.of( true, true ) );
        }

        final Location location = Location.of( "DRRC2" );
        final Metadata meta = Metadata.of( MeasurementUnit.of(),
                                                           DatasetIdentifier.of( location,
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );

        final DichotomousPairs p = (DichotomousPairs) b.addDichotomousData( values ).setMetadata( meta ).build();

        //Check category count
        assertTrue( "Unexpected category count on inputs [2," + p.getCategoryCount() + "].",
                    p.getCategoryCount() == 2 );

        //Check the exceptions 
        //Too many categories
        try
        {
            final DichotomousPairsBuilder c = new DichotomousPairsBuilder();
            final List<MulticategoryPair> multiValues = new ArrayList<>();
            multiValues.add( MulticategoryPair.of( new boolean[] { true, false, false },
                                                   new boolean[] { true, false, false } ) );
            c.setMetadata( meta ).addData( multiValues ).build();
            fail( "Expected a checked exception on invalid inputs." );
        }
        catch ( final Exception e )
        {
        }
        //Valid data
        try
        {
            final DichotomousPairsBuilder c = new DichotomousPairsBuilder();
            final List<MulticategoryPair> multiValues = new ArrayList<>();
            multiValues.add( MulticategoryPair.of( new boolean[] { true, false }, new boolean[] { true, false } ) );
            c.setMetadata( meta ).addData( multiValues ).build();
        }
        catch ( final Exception e )
        {
            fail( "Unexpected exception on valid inputs." );
        }

    }

}

package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.MulticategoryPairs.MulticategoryPairsBuilder;

/**
 * Tests the {@link MulticategoryPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MulticategoryPairsTest
{

    /**
     * Location for testing.
     */
    
    private final Location l1 = Location.of( "DRRC2" );

    /**
     * Metadata for testing.
     */
    
    private final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                           DatasetIdentifier.of( l1, "SQIN", "HEFS" ) );
       
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link MulticategoryPairs}.
     */

    @Test
    public void testMulticategoryPairs()
    {
        final List<MulticategoryPair> values = new ArrayList<>();

        final MulticategoryPairsBuilder b = new MulticategoryPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { true }, new boolean[] { true } ) );
        }

        MulticategoryPairs p = (MulticategoryPairs) b.addData( values ).setMetadata( meta ).build();

        //Check category count
        assertTrue( "Unexpected category count on inputs [2," + p.getCategoryCount() + "].",
                    p.getCategoryCount() == 2 );
        //Check pair count
        assertTrue( "Unexpected pair count for main pairs [10," + p.getRawData().size() + "].",
                    p.getRawData().size() == 10 );
        //Check category count of two when fully expanded
        final MulticategoryPairsBuilder bn = new MulticategoryPairsBuilder();
        values.clear();
        values.add( MulticategoryPair.of( new boolean[] { true, false }, new boolean[] { true, false } ) );
        final MulticategoryPairs q = (MulticategoryPairs) bn.addData( values ).setMetadata( meta ).build();
        assertTrue( "Unexpected category count on inputs [2," + q.getCategoryCount() + "].",
                    q.getCategoryCount() == 2 );
        //Check for no baseline
        assertTrue( "Expected a dataset without a baseline [false," + p.hasBaseline() + "].", !p.hasBaseline() );
        //Check for baseline
        final MulticategoryPairsBuilder bm = new MulticategoryPairsBuilder();
        bm.addData( values ).setMetadata( meta );
        p = (MulticategoryPairs) bm.addDataForBaseline( values ).setMetadataForBaseline( meta ).build(); //Add another
        assertTrue( "Expected a dataset with a baseline [true," + p.hasBaseline() + "].", p.hasBaseline() );
        //Check the metadata
        final SampleMetadata t = SampleMetadata.of();
        b.setMetadata( t );
        p = b.build();
        assertTrue( "Expected non-null metadata.", p.getMetadata().equals( t ) );

    }

    @Test
    public void testExceptionOnConstructionWithNullPair()
    {
        final List<MulticategoryPair> values = Arrays.asList( (MulticategoryPair) null );

        MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();

        exception.expect( SampleDataException.class );

        c.addData( values ).setMetadata( meta ).build();

    }

    @Test
    public void testExceptionOnConstructionWithInconsistentCategoryCount()
    {
        final List<MulticategoryPair> values = new ArrayList<>();

        MulticategoryPairsBuilder c = new MulticategoryPairsBuilder();

        values.add( MulticategoryPair.of( new boolean[] { true, false, false },
                                          new boolean[] { true, false, false } ) );
        values.add( MulticategoryPair.of( new boolean[] { true, false, false, false },
                                          new boolean[] { true, false, false,
                                                          false } ) );
        exception.expect( SampleDataException.class );

        c.addData( values ).setMetadata( meta ).build();
    }


}

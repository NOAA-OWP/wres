package wres.datamodel.inputs.pairs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.Location;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.SafeDichotomousPairs;
import wres.datamodel.inputs.pairs.SafeDichotomousPairs.DichotomousPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link SafeDichotomousPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SafeDichotomousPairsTest
{

    /**
     * Tests the {@link SafeDichotomousPairs}.
     */

    @Test
    public void test1DichotomousPairs()
    {
        final List<VectorOfBooleans> values = new ArrayList<>();

        final DichotomousPairsBuilder b = new DichotomousPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { true, true } ) );
        }

        final Location location = MetadataFactory.getLocation( "DRRC2" );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( location,
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );

        final DichotomousPairs p = (DichotomousPairs) b.addData( values ).setMetadata( meta ).build();

        //Check category count
        assertTrue( "Unexpected category count on inputs [2," + p.getCategoryCount() + "].",
                    p.getCategoryCount() == 2 );

        //Check the exceptions 
        //Too many categories
        try
        {
            values.clear();
            values.add( DataFactory.vectorOf( new boolean[] { true, false, false, true, false, false } ) );
            b.addData( values ).build();
            fail( "Expected a checked exception on invalid inputs." );
        }
        catch ( final Exception e )
        {

        }
        //Valid data
        try
        {
            values.clear();
            values.add( DataFactory.vectorOf( new boolean[] { true, false, true, false } ) );
            b.addData( values ).build();
        }
        catch ( final Exception e )
        {
            fail( "Unexpected exception on valid inputs." );
        }

    }

}

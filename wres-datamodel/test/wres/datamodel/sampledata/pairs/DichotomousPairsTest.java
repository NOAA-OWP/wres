package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.DichotomousPairs.DichotomousPairsBuilder;

/**
 * Tests the {@link DichotomousPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DichotomousPairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                             DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                   "SQIN",
                                                                   "HEFS" ) );
    
    /**
     * Tests the {@link DichotomousPairs#getCategoryCount()}.
     */

    @Test
    public void testGetCategoryCount()
    {
        final List<DichotomousPair> values = new ArrayList<>();

        final DichotomousPairsBuilder b = new DichotomousPairsBuilder();

        for ( int i = 0; i < 10; i++ )
        {
            values.add( DichotomousPair.of( true, true ) );
        }

        final DichotomousPairs p = (DichotomousPairs) b.addDichotomousData( values ).setMetadata( meta ).build();

        //Check category count
        assertTrue( p.getCategoryCount() == 2 );
    }

    /**
     * Tests for an expected exception on construction with too many categories.
     */

    @Test
    public void testExceptedExceptionOnConstructionWithTooManyCategories()
    {
        //Check the exception
        final DichotomousPairsBuilder c = new DichotomousPairsBuilder();
        final List<MulticategoryPair> multiValues = new ArrayList<>();
        multiValues.add( MulticategoryPair.of( new boolean[] { true, false, false },
                                               new boolean[] { true, false, false } ) );
        
        exception.expect( SampleDataException.class );
        c.setMetadata( meta ).addData( multiValues ).build();
    }

}

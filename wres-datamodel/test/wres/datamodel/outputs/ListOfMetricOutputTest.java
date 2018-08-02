package wres.datamodel.outputs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * Tests the {@link ListOfMetricOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ListOfMetricOutputTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Instance of some output metadata to use in testing.
     */

    private MetricOutputMetadata metadata;


    @Before
    public void runBeforeEachTest()
    {
        metadata = MetricOutputMetadata.of( 0,
                                            MeasurementUnit.of(),
                                            MeasurementUnit.of(),
                                            MetricConstants.BIAS_FRACTION );
    }

    /**
     * Tests the construction of a {@link ListOfMetricOutput} using the 
     * {@link ListOfMetricOutput#of(List, MetricOutputMetadata)}
     */

    @Test
    public void testConstruction()
    {
        assertNotNull( ListOfMetricOutput.of( Collections.emptyList(), null ) );
    }

    /**
     * Tests the iteration cannot lead to mutation of the output list.
     */

    @Test
    public void testIteratorCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), null );

        // Removing an element throws an exception
        list.iterator().remove();
    }

    /**
     * Tests that the expected metadata is returned by {@link ListOfMetricOutput#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList(), metadata );
        
        assertTrue( list.getMetadata().equals( metadata ) );
    }
    
    /**
     * Tests that the expected data is returned by {@link ListOfMetricOutput#getData()}.
     */

    @Test
    public void testGetData()
    {
        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), null );

        assertEquals( list.getData(), Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ) );
    }
    
    /**
     * Tests that mutations to the data returned by {@link ListOfMetricOutput#getData()} are not allowed.
     */

    @Test
    public void testGetDataCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );
        
        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), null );
        
        list.getData().add( DoubleScoreOutput.of( 0.1, metadata ) );
    }

}

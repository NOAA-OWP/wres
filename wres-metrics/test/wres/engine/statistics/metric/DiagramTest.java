package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.Objects;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram.QuantileQuantileDiagramBuilder;

/**
 * Tests the {@link Diagram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DiagramTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Output factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest()
    {
        outF = DefaultDataFactory.getInstance();
    }    

    /**
     * Constructs a {@link Diagram} and compares the actual result to the expected result.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testDiagram() throws MetricParameterException
    {
        
    }    
    
    /**
     * Constructs a {@link Diagram} and compares the actual result to the expected result.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testToString() throws MetricParameterException
    {
        QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagramBuilder();
        b.setOutputFactory( outF );
        QuantileQuantileDiagram qqd = b.build();

        assertTrue( "QUANTILE QUANTILE DIAGRAM".equals( qqd.toString() ) );
    }     
    
    /**
     * Checks that {@link Diagram#getDataFactory()} returns a non-null data factory.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testGetDataFactory() throws MetricParameterException
    {
        QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagramBuilder();
        b.setOutputFactory( outF );
        QuantileQuantileDiagram qqd = b.build();

        assertTrue( Objects.nonNull( qqd.getDataFactory() ) );
    }  
    
    /**
     * Checks for an expected exception on building an {@link Diagram} with a null builder.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullBuilder() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct the metric with a null builder." );

        class MockDiagram extends Diagram<SingleValuedPairs, DoubleScoreOutput>
        {

            protected MockDiagram( DiagramBuilder<SingleValuedPairs, DoubleScoreOutput> builder )
                    throws MetricParameterException
            {
                super( null );
            }

            @Override
            public DoubleScoreOutput apply( SingleValuedPairs s )
            {
                return null;
            }

            @Override
            public MetricConstants getID()
            {
                return null;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }
        
        // Throws exception to test
        new MockDiagram( null );
    }
    
    /**
     * Checks for an expected exception on building an {@link Diagram} with a null data factory.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptions() throws MetricParameterException
    {
        //Build the metric
        QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagramBuilder();

        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify a data factory with which to build the metric." );
        
        b.build();
    }


}

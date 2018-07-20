package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
        QuantileQuantileDiagram qqd = b.build();

        assertTrue( "QUANTILE QUANTILE DIAGRAM".equals( qqd.toString() ) );
    }     

}

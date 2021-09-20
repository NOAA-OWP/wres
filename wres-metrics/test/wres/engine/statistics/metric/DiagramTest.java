package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;

/**
 * Tests the {@link Diagram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DiagramTest
{

    /**
     * Constructs a {@link Diagram} and compares the actual result to the expected result.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testToString()
    {
        QuantileQuantileDiagram qqd = QuantileQuantileDiagram.of();

        assertEquals( "QUANTILE QUANTILE DIAGRAM", qqd.toString() );
    }     

}

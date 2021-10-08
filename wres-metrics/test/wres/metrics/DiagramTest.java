package wres.metrics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import wres.metrics.singlevalued.QuantileQuantileDiagram;

/**
 * Tests the {@link Diagram}.
 * 
 * @author James Brown
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

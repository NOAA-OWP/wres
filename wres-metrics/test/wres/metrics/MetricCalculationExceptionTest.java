package wres.metrics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the {@link MetricCalculationException}.
 * 
 * @author James Brown
 */
public final class MetricCalculationExceptionTest
{

    /**
     * Constructs and tests a {@link MetricCalculationException}.
     */

    @Test
    public void testMetricCalculationException()
    {
        final MetricCalculationException e = new MetricCalculationException();
        final MetricCalculationException f = new MetricCalculationException( "Test exception." );
        assertEquals( "Test exception.", new MetricCalculationException( f.getMessage(), e ).getMessage() );
    }
}

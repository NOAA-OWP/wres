package wres.engine.statistics.metric;

import org.junit.Test;

/**
 * Tests the {@link MetricCalculationException}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricCalculationExceptionTest
{

    /**
     * Constructs and tests a {@link MetricCalculationException}.
     */

    @Test
    public void test1MetricCalculationException()
    {
        final MetricCalculationException e = new MetricCalculationException();
        final MetricCalculationException f = new MetricCalculationException("Test exception.");
        new MetricCalculationException(f.getMessage(), e);
    }
}

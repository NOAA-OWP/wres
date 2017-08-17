package wres.datamodel.metric;

import org.junit.Test;

import wres.datamodel.metric.MetricInputSliceException;

/**
 * Tests the {@link MetricInputSliceException}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class PairExceptionTest
{

    /**
     * Constructs and tests a {@link MetricInputSliceException}.
     */

    @Test
    public void test1MetricCalculationException()
    {
        final MetricInputSliceException e = new MetricInputSliceException();
        final MetricInputSliceException f = new MetricInputSliceException("Test exception.");
        new MetricInputSliceException(f.getMessage(), e);
    }
}

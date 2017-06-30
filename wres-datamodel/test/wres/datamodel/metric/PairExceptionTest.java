package wres.datamodel.metric;

import org.junit.Test;

import wres.datamodel.metric.PairException;

/**
 * Tests the {@link PairException}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class PairExceptionTest
{

    /**
     * Constructs and tests a {@link PairException}.
     */

    @Test
    public void test1MetricCalculationException()
    {
        final PairException e = new PairException();
        final PairException f = new PairException("Test exception.");
        new PairException(f.getMessage(), e);
    }
}

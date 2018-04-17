package wres.engine.statistics.metric;

import org.junit.Test;

/**
 * Tests the {@link MetricParameterException}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricParameterExceptionTest
{

    /**
     * Constructs and tests a {@link MetricParameterException}.
     */

    @Test
    public void test1MetricConfigurationException()
    {
        final MetricParameterException e = new MetricParameterException();
        final MetricParameterException f = new MetricParameterException("Test exception.");
        new MetricParameterException(f.getMessage(), e);
    }
}

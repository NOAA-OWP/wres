package wres.engine.statistics.metric.config;

import org.junit.Test;

import wres.engine.statistics.metric.config.MetricConfigurationException;

/**
 * Tests the {@link MetricConfigurationException}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricConfigurationExceptionTest
{

    /**
     * Constructs and tests a {@link MetricConfigurationException}.
     */

    @Test
    public void test1MetricConfigurationException()
    {
        final MetricConfigurationException e = new MetricConfigurationException();
        final MetricConfigurationException f = new MetricConfigurationException("Test exception.");
        new MetricConfigurationException(f.getMessage(), e);
    }
}

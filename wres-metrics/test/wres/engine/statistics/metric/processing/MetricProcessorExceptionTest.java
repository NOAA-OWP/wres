package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertTrue;

import java.util.Objects;

import org.junit.Test;

import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link MetricProcessorException}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorExceptionTest
{

    /**
     * Constructs and tests a {@link MetricParameterException}.
     */

    @Test
    public void testMetricConfigurationException()
    {
        assertTrue( Objects.nonNull( new MetricProcessorException() ) );
        
        MetricProcessorException f = new MetricProcessorException("Test exception.");
        assertTrue( Objects.nonNull( f ) );
        assertTrue( f.getMessage().equals( "Test exception." ) );
        
        MetricProcessorException g = new MetricProcessorException(f.getMessage(), f);
        
        assertTrue( Objects.nonNull( f ) );
        assertTrue( g.getMessage().equals( "Test exception." ) );
        assertTrue( g.getCause().equals( f ) );
    }
}

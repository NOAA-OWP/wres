package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.Objects;

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
    public void testMetricConfigurationException()
    {
        assertTrue( Objects.nonNull( new MetricParameterException() ) );
        
        MetricParameterException f = new MetricParameterException("Test exception.");
        assertTrue( Objects.nonNull( f ) );
        assertTrue( f.getMessage().equals( "Test exception." ) );
        
        MetricParameterException g = new MetricParameterException(f.getMessage(), f);
        
        assertTrue( Objects.nonNull( f ) );
        assertTrue( g.getMessage().equals( "Test exception." ) );
        assertTrue( g.getCause().equals( f ) );
    }
}

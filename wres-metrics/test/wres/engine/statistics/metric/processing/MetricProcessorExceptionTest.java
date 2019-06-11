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
     * Expected exception message.
     */
    
    private static final String TEST_EXCEPTION = "Test exception.";

    /**
     * Constructs and tests a {@link MetricParameterException}.
     */

    @Test
    public void testMetricConfigurationException()
    {
        assertTrue( Objects.nonNull( new MetricProcessorException() ) );
        
        MetricProcessorException f = new MetricProcessorException(TEST_EXCEPTION);
        assertTrue( Objects.nonNull( f ) );
        assertTrue( f.getMessage().equals( TEST_EXCEPTION ) );
        
        MetricProcessorException g = new MetricProcessorException(f.getMessage(), f);
        
        assertTrue( Objects.nonNull( f ) );
        assertTrue( g.getMessage().equals( TEST_EXCEPTION ) );
        assertTrue( g.getCause().equals( f ) );
    }
}

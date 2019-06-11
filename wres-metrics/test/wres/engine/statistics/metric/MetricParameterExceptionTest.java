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
     * Expected exception message.
     */
    
    private static final String TEST_EXCEPTION = "Test exception.";

    /**
     * Constructs and tests a {@link MetricParameterException}.
     */

    @Test
    public void testMetricConfigurationException()
    {
        assertTrue( Objects.nonNull( new MetricParameterException() ) );
        
        MetricParameterException f = new MetricParameterException(TEST_EXCEPTION);
        assertTrue( Objects.nonNull( f ) );
        assertTrue( f.getMessage().equals( TEST_EXCEPTION ) );
        
        MetricParameterException g = new MetricParameterException(f.getMessage(), f);
        
        assertTrue( Objects.nonNull( f ) );
        assertTrue( g.getMessage().equals( TEST_EXCEPTION ) );
        assertTrue( g.getCause().equals( f ) );
    }
}

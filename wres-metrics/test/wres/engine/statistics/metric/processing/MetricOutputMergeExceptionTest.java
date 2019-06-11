package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertTrue;

import java.util.Objects;

import org.junit.Test;

/**
 * Tests the {@link MetricOutputMergeException}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricOutputMergeExceptionTest
{

    /**
     * Expected exception message.
     */
    
    private static final String TEST_EXCEPTION = "Test exception.";

    /**
     * Constructs and tests a {@link MetricOutputMergeException}.
     */

    @Test
    public void testMetricOutputMergeExceptionException()
    {
        assertTrue( Objects.nonNull( new MetricOutputMergeException() ) );
        
        MetricOutputMergeException f = new MetricOutputMergeException(TEST_EXCEPTION);
        assertTrue( Objects.nonNull( f ) );
        assertTrue( f.getMessage().equals( TEST_EXCEPTION ) );
        
        MetricOutputMergeException g = new MetricOutputMergeException(f.getMessage(), f);
        
        assertTrue( Objects.nonNull( f ) );
        assertTrue( g.getMessage().equals( TEST_EXCEPTION ) );
        assertTrue( g.getCause().equals( f ) );
    }
}

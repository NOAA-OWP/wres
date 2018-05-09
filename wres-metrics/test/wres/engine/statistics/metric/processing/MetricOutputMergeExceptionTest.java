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
     * Constructs and tests a {@link MetricOutputMergeException}.
     */

    @Test
    public void testMetricOutputMergeExceptionException()
    {
        assertTrue( Objects.nonNull( new MetricOutputMergeException() ) );
        
        MetricOutputMergeException f = new MetricOutputMergeException("Test exception.");
        assertTrue( Objects.nonNull( f ) );
        assertTrue( f.getMessage().equals( "Test exception." ) );
        
        MetricOutputMergeException g = new MetricOutputMergeException(f.getMessage(), f);
        
        assertTrue( Objects.nonNull( f ) );
        assertTrue( g.getMessage().equals( "Test exception." ) );
        assertTrue( g.getCause().equals( f ) );
    }
}

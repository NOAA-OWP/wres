package wres.pipeline.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

        MetricOutputMergeException f = new MetricOutputMergeException( TEST_EXCEPTION );
        assertNotNull( f );
        assertEquals( TEST_EXCEPTION, f.getMessage() );

        MetricOutputMergeException g = new MetricOutputMergeException( f.getMessage(), f );

        assertNotNull( g );
        assertEquals( TEST_EXCEPTION, g.getMessage() );
        assertEquals( f, g.getCause() );
    }
}

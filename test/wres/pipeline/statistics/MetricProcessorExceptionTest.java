package wres.pipeline.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

        MetricProcessorException f = new MetricProcessorException( TEST_EXCEPTION );
        assertNotNull( f );
        assertEquals( TEST_EXCEPTION, f.getMessage() );

        MetricProcessorException g = new MetricProcessorException( f.getMessage(), f );

        assertNotNull( g );
        assertEquals( TEST_EXCEPTION, g.getMessage() );
        assertEquals( f, g.getCause() );
    }
}

package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

        MetricParameterException f = new MetricParameterException( TEST_EXCEPTION );
        assertNotNull( f );
        assertEquals( TEST_EXCEPTION, f.getMessage() );

        MetricParameterException g = new MetricParameterException( f.getMessage(), f );

        assertNotNull( g );
        assertEquals( TEST_EXCEPTION, g.getMessage() );
        assertEquals( f, g.getCause() );
    }
}

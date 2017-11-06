package wres.datamodel;

import org.junit.Test;

import wres.datamodel.inputs.MetricInputException;

/**
 * Tests the {@link MetricInputExceptionException}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricInputExceptionTest
{

    /**
     * Constructs and tests a {@link MetricInputException}.
     */

    @Test
    public void test1MetricCalculationException()
    {
        final MetricInputException e = new MetricInputException();
        final MetricInputException f = new MetricInputException("Test exception.");
        new MetricInputException(f.getMessage(), e);
    }
}

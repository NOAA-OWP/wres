package wres.datamodel.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.datamodel.thresholds.ThresholdsByMetric;

class MetricsTest
{

    /** Instance to test. */
    private Metrics testInstance;

    @BeforeEach
    private void runBeforeEachTest()
    {
        ThresholdsByMetric thresholdsByMetric = Mockito.mock( ThresholdsByMetric.class );
        Mockito.when( thresholdsByMetric.hasThresholdsForTheseMetrics() )
               .thenReturn( Set.of( MetricConstants.MEAN_ERROR ) );
        this.testInstance = Metrics.of( thresholdsByMetric, 7 );
    }

    @Test
    void testGetMinimumSampleSize()
    {
        assertEquals( 7, this.testInstance.getMinimumSampleSize() );
    }

    @Test
    void testGetMetrics()
    {
        assertEquals( Set.of( MetricConstants.MEAN_ERROR ), this.testInstance.getMetrics() );
    }

    @Test
    void testGetThresholds()
    {
        Set<MetricConstants> actual = this.testInstance.getThresholdsByMetric()
                                                       .hasThresholdsForTheseMetrics();
        assertEquals( Set.of( MetricConstants.MEAN_ERROR ), actual );
    }

}

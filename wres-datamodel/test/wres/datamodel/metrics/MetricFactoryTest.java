package wres.datamodel.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import wres.config.generated.MetricConfigName;
import wres.config.generated.TimeSeriesMetricConfigName;

/**
 * Tests the {@link MetricFactory}.
 * 
 * @author James Brown
 *
 */

class MetricFactoryTest
{

    /**
     * Tests the {@link MetricFactory#getMetricName(wres.config.generated.MetricConfigName)}.
     */

    @Test
    void testGetMetricName()
    {
        // The MetricConfigName.ALL_VALID returns null        
        assertNull( MetricFactory.getMetricName( MetricConfigName.ALL_VALID ) );

        // Check that a mapping exists in MetricConstants for all entries in the MetricConfigName
        for ( MetricConfigName next : MetricConfigName.values() )
        {
            if ( next != MetricConfigName.ALL_VALID )
            {
                assertNotNull( MetricFactory.getMetricName( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricFactory#getMetricName(wres.config.generated.MetricConfigName)} throws an 
     * expected exception when the input is null.
     */

    @Test
    void testGetMetricNameThrowsNPEWhenInputIsNull()
    {
        assertThrows( NullPointerException.class, () -> MetricFactory.getMetricName( (MetricConfigName) null ) );
    }

    /**
     * Tests the {@link MetricFactory#getMetricName(wres.config.generated.TimeSeriesMetricConfigName)}.
     */

    @Test
    void testGetTimeSeriesMetricName()
    {
        // The TimeSeriesMetricConfigName.ALL_VALID returns null        
        assertNull( MetricFactory.getMetricName( TimeSeriesMetricConfigName.ALL_VALID ) );

        // Check that a mapping exists in MetricConstants for all entries in the TimeSeriesMetricConfigName
        for ( TimeSeriesMetricConfigName next : TimeSeriesMetricConfigName.values() )
        {
            if ( next != TimeSeriesMetricConfigName.ALL_VALID )
            {
                assertNotNull( MetricFactory.getMetricName( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricFactory#getMetricName(wres.config.generated.TimeSeriesMetricConfigName)} throws an 
     * expected exception when the input is null.
     */

    @Test
    void testGetTimeSeriesMetricNameThrowsNPEWhenInputIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> MetricFactory.getMetricName( (TimeSeriesMetricConfigName) null ) );
    }
}

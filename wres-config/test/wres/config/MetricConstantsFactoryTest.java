package wres.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.MetricConfigException;
import wres.config.MetricConstants;
import wres.config.MetricConstantsFactory;
import wres.config.generated.DestinationConfig;
import wres.config.generated.EnsembleAverageType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.MetricConstants.StatisticType;

/**
 * Tests the {@link MetricConstantsFactory}.
 * 
 * @author James Brown
 *
 */

class MetricConstantsFactoryTest
{
    /** Test thresholds. */
    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

    /** Default mocked project configuration. */
    private ProjectConfig defaultMockedConfig;

    /**
     * Tests the {@link MetricConstantsFactory#getMetricName(wres.config.generated.MetricConfigName)}.
     */

    @Test
    void testGetMetricName()
    {
        // The MetricConfigName.ALL_VALID returns null        
        assertNull( MetricConstantsFactory.getMetricName( MetricConfigName.ALL_VALID ) );

        // Check that a mapping exists in MetricConstants for all entries in the MetricConfigName
        for ( MetricConfigName next : MetricConfigName.values() )
        {
            if ( next != MetricConfigName.ALL_VALID )
            {
                assertNotNull( MetricConstantsFactory.getMetricName( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricConstantsFactory#getMetricName(wres.config.generated.MetricConfigName)} throws an 
     * expected exception when the input is null.
     */

    @Test
    void testGetMetricNameThrowsNPEWhenInputIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> MetricConstantsFactory.getMetricName( (MetricConfigName) null ) );
    }

    /**
     * Tests the {@link MetricConstantsFactory#getMetricName(wres.config.generated.TimeSeriesMetricConfigName)}.
     */

    @Test
    void testGetTimeSeriesMetricName()
    {
        // The TimeSeriesMetricConfigName.ALL_VALID returns null        
        assertNull( MetricConstantsFactory.getMetricName( TimeSeriesMetricConfigName.ALL_VALID ) );

        // Check that a mapping exists in MetricConstants for all entries in the TimeSeriesMetricConfigName
        for ( TimeSeriesMetricConfigName next : TimeSeriesMetricConfigName.values() )
        {
            if ( next != TimeSeriesMetricConfigName.ALL_VALID )
            {
                assertNotNull( MetricConstantsFactory.getMetricName( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricConstantsFactory#getMetricName(wres.config.generated.TimeSeriesMetricConfigName)} throws an 
     * expected exception when the input is null.
     */

    @Test
    void testGetTimeSeriesMetricNameThrowsNPEWhenInputIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> MetricConstantsFactory.getMetricName( (TimeSeriesMetricConfigName) null ) );
    }

    @BeforeEach
    void setUpBeforeEachTest()
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        this.defaultMockedConfig =
                new ProjectConfig( null,
                                   null,
                                   List.of( new MetricsConfig( thresholds,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
                                   new Outputs( List.of( new DestinationConfig( OutputTypeSelection.THRESHOLD_LEAD,
                                                                                null,
                                                                                null,
                                                                                null,
                                                                                null ) ),
                                                null ),
                                   null,
                                   null );
    }

    /**
     * Tests the {@link MetricConstantsFactory#from(wres.config.generated.MetricConfigName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    void testFromMetricName()
    {
        //Check for mapping without exception
        for ( MetricConfigName nextConfig : MetricConfigName.values() )
        {
            if ( nextConfig != MetricConfigName.ALL_VALID )
            {
                assertTrue( Objects.nonNull( MetricConstantsFactory.from( nextConfig ) ),
                            "No mapping found for '" + nextConfig + "'." );
            }
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( Objects.isNull( MetricConstantsFactory.from( MetricConfigName.ALL_VALID ) ),
                    "Expected a null mapping for '" + MetricConfigName.ALL_VALID
                                                                                                 + "'." );
    }

    /**
     * Tests the {@link MetricConstantsFactory#from(MetricConfigName)} for a checked exception on null input.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    void testExceptionFromMetricNameWithNullInput()
    {
        assertThrows( NullPointerException.class,
                      () -> MetricConstantsFactory.from( (MetricConfigName) null ) );
    }

    /**
     * Tests the {@link MetricConstantsFactory#from(wres.config.generated.SummaryStatisticsName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    void testFromSummaryStatisticsName()
    {
        //Check for mapping without exception
        for ( SummaryStatisticsName nextStat : SummaryStatisticsName.values() )
        {
            if ( nextStat != SummaryStatisticsName.ALL_VALID )
            {
                assertTrue( Objects.nonNull( MetricConstantsFactory.from( nextStat ) ),
                            "No mapping found for '" + nextStat + "'." );
            }
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( Objects.isNull( MetricConstantsFactory.from( SummaryStatisticsName.ALL_VALID ) ),
                    "Expected a null mapping for '" + SummaryStatisticsName.ALL_VALID + "'." );
    }

    /**
     * Tests the {@link MetricConstantsFactory#from(SummaryStatisticsName)} for a checked exception on null input.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    void testExceptionFromSummaryStatisticsNameWithNullInput()
    {
        assertThrows( NullPointerException.class,
                      () -> MetricConstantsFactory.from( (SummaryStatisticsName) null ) );
    }

    /**
     * Tests the {@link MetricConstantsFactory#getMetricsFromConfig(wres.config.generated.ProjectConfig)} by comparing
     * actual results to expected results.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    void testGetMetricsFromConfig()
    {
        assertEquals( Set.of( MetricConstants.BIAS_FRACTION,
                              MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                              MetricConstants.MEAN_ERROR ),
                      MetricConstantsFactory.getMetricsFromConfig( this.defaultMockedConfig ) );
    }

    /**
     * Tests the {@link MetricConstantsFactory#hasTheseOutputsByThresholdLead(ProjectConfig, 
     * MetricConstants.StatisticType)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    void testHasTheseOutputsByThresholdLead()
    {
        // Outputs by threshold and lead time required
        assertTrue( MetricConstantsFactory.hasTheseOutputsByThresholdLead( defaultMockedConfig,
                                                                           StatisticType.DOUBLE_SCORE ) );

        // No outputs configuration defined
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   List.of( new MetricsConfig( null,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
                                   null,
                                   null,
                                   null );

        assertFalse( MetricConstantsFactory.hasTheseOutputsByThresholdLead( mockedConfig,
                                                                            StatisticType.DOUBLE_SCORE ) );

        // Output configuration defined, but is not by threshold then lead
        ProjectConfig mockedConfigWithOutput =
                new ProjectConfig( null,
                                   null,
                                   List.of( new MetricsConfig( null,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
                                   new Outputs( List.of( new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                                                null,
                                                                                null,
                                                                                null,
                                                                                null ) ),
                                                null ),
                                   null,
                                   null );

        assertFalse( MetricConstantsFactory.hasTheseOutputsByThresholdLead( mockedConfigWithOutput,
                                                                            StatisticType.DOUBLE_SCORE ) );

        // No output and no output groups of the specified type
        assertFalse( MetricConstantsFactory.hasTheseOutputsByThresholdLead( mockedConfig,
                                                                            StatisticType.DIAGRAM ) );

    }
}

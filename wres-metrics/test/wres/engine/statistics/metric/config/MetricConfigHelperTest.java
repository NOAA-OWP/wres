package wres.engine.statistics.metric.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.SummaryStatisticsConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.datamodel.thresholds.ThresholdsByMetric;

/**
 * Tests the {@link MetricConfigHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricConfigHelperTest
{

    /**
     * Test thresholds.
     */
    
    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default mocked project configuration.
     */

    private ProjectConfig defaultMockedConfig;

    /**
     * Set-up.
     */

    @Before
    public void setUpBeforeEachTest()
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        defaultMockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( OutputTypeSelection.THRESHOLD_LEAD,
                                                                                      null,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ),
                                                null ),
                                   null,
                                   null );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(wres.config.generated.MetricConfigName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testFromMetricName()
    {
        //Check for mapping without exception
        for ( MetricConfigName nextConfig : MetricConfigName.values() )
        {
            if ( nextConfig != MetricConfigName.ALL_VALID )
            {
                assertTrue( "No mapping found for '" + nextConfig
                            + "'.",
                            Objects.nonNull( MetricConfigHelper.from( nextConfig ) ) );
            }
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( "Expected a null mapping for '" + MetricConfigName.ALL_VALID
                    + "'.",
                    Objects.isNull( MetricConfigHelper.from( MetricConfigName.ALL_VALID ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(MetricConfigName)} for a checked exception on null input.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testExceptionFromMetricNameWithNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify input configuration with a non-null name to map" );
        MetricConfigHelper.from( (MetricConfigName) null );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(wres.config.generated.SummaryStatisticsName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testFromSummaryStatisticsName()
    {
        //Check for mapping without exception
        for ( SummaryStatisticsName nextStat : SummaryStatisticsName.values() )
        {
            if ( nextStat != SummaryStatisticsName.ALL_VALID )
            {
                assertTrue( "No mapping found for '" + nextStat
                            + "'.",
                            Objects.nonNull( MetricConfigHelper.from( nextStat ) ) );
            }
        }

        //Check the MetricConfigName.ALL_VALID       
        assertTrue( "Expected a null mapping for '" + SummaryStatisticsName.ALL_VALID
                    + "'.",
                    Objects.isNull( MetricConfigHelper.from( SummaryStatisticsName.ALL_VALID ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(SummaryStatisticsName)} for a checked exception on null input.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testExceptionFromSummaryStatisticsNameWithNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify input configuration with a non-null name to map" );
        MetricConfigHelper.from( (SummaryStatisticsName) null );
    }

    /**
     * Tests the {@link MetricConfigHelper#getMetricsFromConfig(wres.config.generated.ProjectConfig)} by comparing
     * actual results to expected results.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetMetricsFromConfig()
    {
        assertTrue( MetricConfigHelper.getMetricsFromConfig( defaultMockedConfig )
                                      .equals( new HashSet<>( Arrays.asList( MetricConstants.BIAS_FRACTION,
                                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                             MetricConstants.MEAN_ERROR ) ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#getThresholdsFromConfig(ProjectConfig, wres.datamodel.DataFactory, 
     * java.util.Collection)} by comparing actual results to expected results for a scenario where external thresholds
     * are defined, together with a dimension for value thresholds.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetThresholdsFromConfigWithExternalThresholdsAndDimension()
    {

        // Obtain the threshold dimension
        MeasurementUnit dimension = MeasurementUnit.of( "CMS" );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "0.1,0.2",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   new PairConfig( "CMS",
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Mock external thresholds
        Map<MetricConstants, Set<Threshold>> mockExternal = new EnumMap<>( MetricConstants.class );
        Set<Threshold> atomicExternal = new HashSet<>();
        atomicExternal.add( Threshold.of( OneOrTwoDoubles.of( 0.3 ),
                                                     Operator.GREATER,
                                                     ThresholdConstants.ThresholdDataType.LEFT,
                                                     dimension ) );
        mockExternal.put( MetricConstants.BIAS_FRACTION, atomicExternal );

        ThresholdsByMetric externalThresholds =
                new ThresholdsByMetricBuilder().addThresholds( mockExternal, ThresholdGroup.VALUE ).build();

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = MetricConfigHelper.getThresholdsFromConfig( mockedConfig,
                                                                                        externalThresholds );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new EnumMap<>( MetricConstants.class );
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                              Operator.GREATER,
                                                                              ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 0.1 ),
                                                                              Operator.GREATER,
                                                                              ThresholdConstants.ThresholdDataType.LEFT,
                                                                              dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 0.2 ),
                                                                              Operator.GREATER,
                                                                              ThresholdConstants.ThresholdDataType.LEFT,
                                                                              dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 0.3 ),
                                                                              Operator.GREATER,
                                                                              ThresholdConstants.ThresholdDataType.LEFT,
                                                                              dimension ) ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholds );

        assertTrue( actual.equals( expected ) );
    }

    /**
     * Tests a method with private scope in {@link MetricConfigHelper} using thresholds with a 
     * {@link Operator#BETWEEN} condition. TODO: expose this and test via 
     * {@link MetricConfigHelper#getThresholdsFromConfig(ProjectConfig, DataFactory, java.util.Collection)} once 
     * the {@link ProjectConfig} supports thresholds with a {@link Operator#BETWEEN} condition.
     * @throws MetricConfigException if an unexpected exception is encountered
     * @throws SecurityException if the reflection fails via a security manager
     * @throws NoSuchMethodException if a matching method is not found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalArgumentException if the instance method could not be invoked with the supplied arguments
     * @throws IllegalAccessException if this Method object is enforcing Java language access control and the 
     *            underlying method is inaccessible.
     */

    @Test
    public void testGetThresholdsFromConfigWithBetweenCondition() throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {

        Method method =
                ThresholdsGenerator.class.getDeclaredMethod( "getThresholdsFromCommaSeparatedValues",
                                                            String.class,
                                                            Operator.class,
                                                            ThresholdConstants.ThresholdDataType.class,
                                                            boolean.class,
                                                            MeasurementUnit.class );
        method.setAccessible( true );

        // Test with probability thresholds
        @SuppressWarnings( "unchecked" )
        Set<Threshold> actual = (Set<Threshold>) method.invoke( null,
                                                                TEST_THRESHOLDS,
                                                                Operator.BETWEEN,
                                                                ThresholdConstants.ThresholdDataType.LEFT,
                                                                true,
                                                                null );

        Set<Threshold> expected = new HashSet<>();
        expected.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1, 0.2 ),
                                                          Operator.BETWEEN,
                                                          ThresholdConstants.ThresholdDataType.LEFT ) );
        expected.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2, 0.3 ),
                                                          Operator.BETWEEN,
                                                          ThresholdConstants.ThresholdDataType.LEFT ) );
        assertTrue( actual.equals( expected ) );

        // Test with value thresholds
        @SuppressWarnings( "unchecked" )
        Set<Threshold> actualValue = (Set<Threshold>) method.invoke( null,
                                                                     TEST_THRESHOLDS,
                                                                     Operator.BETWEEN,
                                                                     ThresholdConstants.ThresholdDataType.LEFT,
                                                                     false,
                                                                     null );

        Set<Threshold> expectedValue = new HashSet<>();
        expectedValue.add( Threshold.of( OneOrTwoDoubles.of( 0.1, 0.2 ),
                                                    Operator.BETWEEN,
                                                    ThresholdConstants.ThresholdDataType.LEFT ) );
        expectedValue.add( Threshold.of( OneOrTwoDoubles.of( 0.2, 0.3 ),
                                                    Operator.BETWEEN,
                                                    ThresholdConstants.ThresholdDataType.LEFT ) );


        assertTrue( actualValue.equals( expectedValue ) );

        // Test exception    
        exception.expectCause( CoreMatchers.isA( MetricConfigException.class ) );
        method.invoke( null,
                       "0.1",
                       Operator.BETWEEN,
                       ThresholdConstants.ThresholdDataType.LEFT,
                       false,
                       null );
    }

    /**
     * Tests the {@link MetricConfigHelper#hasSummaryStatisticsFor(ProjectConfig, java.util.function.Predicate)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testHasSummaryStatisticsForNamedMetric()
    {
        // Mock some metrics
        List<TimeSeriesMetricConfig> timeSeriesMetrics = new ArrayList<>();

        // Mock some summary statistics
        List<SummaryStatisticsName> stats = new ArrayList<>();
        stats.add( SummaryStatisticsName.MAXIMUM );
        stats.add( SummaryStatisticsName.MINIMUM );
        stats.add( SummaryStatisticsName.MEAN );

        SummaryStatisticsConfig statsConfig = new SummaryStatisticsConfig( stats );

        timeSeriesMetrics.add( new TimeSeriesMetricConfig( null,
                                                           null,
                                                           TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR,
                                                           statsConfig ) );

        MetricsConfig metrics = new MetricsConfig( null, null, timeSeriesMetrics );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( metrics ),
                                   null,
                                   null,
                                   null );

        // Summary statistics expected
        assertTrue( MetricConfigHelper.hasSummaryStatisticsFor( mockedConfig,
                                                                next -> next.equals( TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR ) ) );

        //No summary statistics expected
        ProjectConfig mockedEmptyConfig =
                new ProjectConfig( null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null );

        // Summary statistics expected
        assertFalse( MetricConfigHelper.hasSummaryStatisticsFor( mockedEmptyConfig,
                                                                 next -> next.equals( TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#hasSummaryStatisticsFor(ProjectConfig, java.util.function.Predicate)} using
     * configuration that has {@link TimeSeriesMetricConfigName#ALL_VALID }
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testHasSummaryStatisticsForAllValid()
    {
        // Mock some metrics
        List<TimeSeriesMetricConfig> timeSeriesMetrics = new ArrayList<>();

        // Mock some summary statistics
        List<SummaryStatisticsName> stats = new ArrayList<>();
        stats.add( SummaryStatisticsName.ALL_VALID );

        SummaryStatisticsConfig statsConfig = new SummaryStatisticsConfig( stats );

        timeSeriesMetrics.add( new TimeSeriesMetricConfig( null,
                                                           null,
                                                           TimeSeriesMetricConfigName.ALL_VALID,
                                                           statsConfig ) );

        MetricsConfig metrics = new MetricsConfig( null, null, timeSeriesMetrics );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( metrics ),
                                   null,
                                   null,
                                   null );

        // Summary statistics expected
        assertTrue( MetricConfigHelper.hasSummaryStatisticsFor( mockedConfig,
                                                                next -> next.equals( TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR ) ) );

        // Null summary statistics
        timeSeriesMetrics.clear();

        timeSeriesMetrics.add( new TimeSeriesMetricConfig( null,
                                                           null,
                                                           TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR,
                                                           null ) );

        MetricsConfig metricsNullStats = new MetricsConfig( null, null, timeSeriesMetrics );

        ProjectConfig mockedConfigNullStats =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( metricsNullStats ),
                                   null,
                                   null,
                                   null );

        // No summary statistics expected
        assertFalse( MetricConfigHelper.hasSummaryStatisticsFor( mockedConfigNullStats,
                                                                 next -> next.equals( TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR ) ) );

        // No summary statistics expected, because metric not there
        assertFalse( MetricConfigHelper.hasSummaryStatisticsFor( mockedConfigNullStats,
                                                                 next -> next.equals( TimeSeriesMetricConfigName.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#hasSummaryStatisticsFor(ProjectConfig, java.util.function.Predicate)} using
     * a predicate that tests for {@link TimeSeriesMetricConfigName#ALL_VALID }, which is not allowed.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testHasSummaryStatisticsThrowsExceptionOnAllValid()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot obtain summary statistics for the general type 'all valid' "
                                 + "when a specific type is required: instead, provide a time-series "
                                 + "metric that is specific." );

        MetricConfigHelper.hasSummaryStatisticsFor( defaultMockedConfig,
                                                    next -> next.equals( TimeSeriesMetricConfigName.ALL_VALID ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#hasTheseOutputsByThresholdLead(ProjectConfig, 
     * wres.datamodel.MetricConstants.StatisticType)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testHasTheseOutputsByThresholdLead()
    {
        // Outputs by threshold and lead time required
        assertTrue( MetricConfigHelper.hasTheseOutputsByThresholdLead( defaultMockedConfig,
                                                                       StatisticType.DOUBLE_SCORE ) );

        // No outputs configuration defined
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        assertFalse( MetricConfigHelper.hasTheseOutputsByThresholdLead( mockedConfig,
                                                                        StatisticType.DOUBLE_SCORE ) );

        // Output configuration defined, but is not by threshold then lead
        ProjectConfig mockedConfigWithOutput =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                                                      null,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ),
                                                null ),
                                   null,
                                   null );

        assertFalse( MetricConfigHelper.hasTheseOutputsByThresholdLead( mockedConfigWithOutput,
                                                                        StatisticType.DOUBLE_SCORE ) );

        // No output and no output groups of the specified type
        assertFalse( MetricConfigHelper.hasTheseOutputsByThresholdLead( mockedConfig,
                                                                        StatisticType.MULTIVECTOR ) );

    }

    /**
     * Tests the {@link MetricConfigHelper#getCacheListFromProjectConfig(ProjectConfig)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testGetCachedListFromProjectConfig()
    {
        // No outputs configuration defined
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.RELIABILITY_DIAGRAM ) );

        // Output configuration defined, but is not by threshold then lead
        ProjectConfig mockedConfigWithOutput =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( OutputTypeSelection.THRESHOLD_LEAD,
                                                                                      null,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ),
                                                null ),
                                   null,
                                   null );

        Set<StatisticType> expected = new HashSet<>();
        expected.add( StatisticType.MULTIVECTOR );
        expected.add( StatisticType.PAIRED );
        expected.add( StatisticType.DOUBLE_SCORE );
        expected.add( StatisticType.BOXPLOT_PER_POOL );
        
        assertEquals( expected, MetricConfigHelper.getCacheListFromProjectConfig( mockedConfigWithOutput ) );

    }

}

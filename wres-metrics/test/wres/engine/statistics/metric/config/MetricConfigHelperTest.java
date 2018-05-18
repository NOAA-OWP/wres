package wres.engine.statistics.metric.config;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigException;
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
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;

/**
 * Tests the {@link MetricConfigHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricConfigHelperTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Data factory.
     */

    private DataFactory dataFac;

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
        dataFac = DefaultDataFactory.getInstance();

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        defaultMockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( null,
                                                                                      OutputTypeSelection.THRESHOLD_LEAD,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ) ),
                                   null,
                                   null );
    }

    /**
     * Tests the {@link MetricConfigHelper#from(wres.config.generated.MetricConfigName)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testFromMetricName() throws MetricConfigException
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
    public void testExceptionFromMetricNameWithNullInput() throws MetricConfigException
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
    public void testFromSummaryStatisticsName() throws MetricConfigException
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
    public void testExceptionFromSummaryStatisticsNameWithNullInput() throws MetricConfigException
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
    public void testGetMetricsFromConfig() throws MetricConfigException
    {
        assertTrue( MetricConfigHelper.getMetricsFromConfig( defaultMockedConfig )
                                      .equals( new HashSet<>( Arrays.asList( MetricConstants.BIAS_FRACTION,
                                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                             MetricConstants.MEAN_ERROR ) ) ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#getThresholdsFromConfig(ProjectConfig, wres.datamodel.DataFactory, 
     * java.util.Collection)} by comparing actual results to expected results for a scenario where external thresholds
     * are not defined and no threshold dimension is defined.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetThresholdsFromConfigWithoutExternalThresholdsOrDimension() throws MetricConfigException
    {

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = MetricConfigHelper.getThresholdsFromConfig( defaultMockedConfig,
                                                                                        dataFac,
                                                                                        (ThresholdsByMetric) null );

        Map<MetricConstants, Set<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new HashMap<>();
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.1 ),
                                                                                     Operator.GREATER,
                                                                                     ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.2 ),
                                                                                     Operator.GREATER,
                                                                                     ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.3 ),
                                                                                     Operator.GREATER,
                                                                                     ThresholdConstants.ThresholdDataType.LEFT ) ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholds );
        expected.put( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, atomicThresholds );
        expected.put( MetricConstants.MEAN_ERROR, atomicThresholds );

        assertTrue( actual.equals( expected ) );

        // Check for the same result when specifying an explicit pair configuration with null dimension
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfigWithNullDimension =
                new ProjectConfig( null,
                                   new PairConfig( null,
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

        // Compute combined thresholds
        ThresholdsByMetric actualByMetricNullDimension =
                MetricConfigHelper.getThresholdsFromConfig( mockedConfigWithNullDimension,
                                                            dataFac,
                                                            (ThresholdsByMetric) null );

        Map<MetricConstants, Set<OneOrTwoThresholds>> actualWithNullDim =
                actualByMetricNullDimension.getOneOrTwoThresholds();

        assertTrue( actualWithNullDim.equals( expected ) );
    }

    /**
     * Tests the {@link MetricConfigHelper#getThresholdsFromConfig(ProjectConfig, wres.datamodel.DataFactory, 
     * java.util.Collection)} by comparing actual results to expected results for a scenario where external thresholds
     * are defined, together with a dimension for value thresholds.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetThresholdsFromConfigWithExternalThresholdsAndDimension() throws MetricConfigException
    {

        // Obtain the threshold dimension
        MetadataFactory metaFac = dataFac.getMetadataFactory();
        Dimension dimension = metaFac.getDimension( "CMS" );

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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Mock external thresholds
        Map<MetricConstants, Set<Threshold>> mockExternal = new HashMap<>();
        Set<Threshold> atomicExternal = new HashSet<>();
        atomicExternal.add( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.3 ),
                                                 Operator.GREATER,
                                                 ThresholdConstants.ThresholdDataType.LEFT,
                                                 dimension ) );
        mockExternal.put( MetricConstants.BIAS_FRACTION, atomicExternal );

        ThresholdsByMetric externalThresholds =
                dataFac.ofThresholdsByMetricBuilder().addThresholds( mockExternal, ThresholdGroup.VALUE ).build();

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = MetricConfigHelper.getThresholdsFromConfig( mockedConfig,
                                                                                        dataFac,
                                                                                        externalThresholds );

        Map<MetricConstants, Set<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new HashMap<>();
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.1 ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT,
                                                                          dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.2 ),
                                                                          Operator.GREATER,
                                                                          ThresholdConstants.ThresholdDataType.LEFT,
                                                                          dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.3 ),
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
     * the {@link ProjectConfigException} supports thresholds with a {@link Operator#BETWEEN} condition.
     * @throws MetricConfigException if an unexpected exception is encountered
     * @throws SecurityException if the reflection fails via a security manager
     * @throws NoSuchMethodException if a matching method is not found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalArgumentException if the instance method could not be invoked with the supplied arguments
     * @throws IllegalAccessException if this Method object is enforcing Java language access control and the 
     *            underlying method is inaccessible.
     */

    @Test
    public void testGetThresholdsFromConfigWithBetweenCondition() throws MetricConfigException, NoSuchMethodException,
            SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {

        Method method =
                MetricConfigHelper.class.getDeclaredMethod( "getThresholdsFromCommaSeparatedValues",
                                                            DataFactory.class,
                                                            String.class,
                                                            Operator.class,
                                                            ThresholdConstants.ThresholdDataType.class,
                                                            boolean.class,
                                                            Dimension.class );
        method.setAccessible( true );

        // Test with probability thresholds
        @SuppressWarnings( "unchecked" )
        Set<Threshold> actual = (Set<Threshold>) method.invoke( null,
                                                                new Object[] { dataFac, "0.1,0.2,0.3", Operator.BETWEEN,
                                                                               ThresholdConstants.ThresholdDataType.LEFT,
                                                                               true,
                                                                               null } );

        Set<Threshold> expected = new HashSet<>();
        expected.add( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.1, 0.2 ),
                                                      Operator.BETWEEN,
                                                      ThresholdConstants.ThresholdDataType.LEFT ) );
        expected.add( dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.2, 0.3 ),
                                                      Operator.BETWEEN,
                                                      ThresholdConstants.ThresholdDataType.LEFT ) );
        assertTrue( actual.equals( expected ) );

        // Test with value thresholds
        @SuppressWarnings( "unchecked" )
        Set<Threshold> actualValue = (Set<Threshold>) method.invoke( null,
                                                                     new Object[] { dataFac, "0.1,0.2,0.3",
                                                                                    Operator.BETWEEN,
                                                                                    ThresholdConstants.ThresholdDataType.LEFT,
                                                                                    false,
                                                                                    null } );

        Set<Threshold> expectedValue = new HashSet<>();
        expectedValue.add( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.1, 0.2 ),
                                                Operator.BETWEEN,
                                                ThresholdConstants.ThresholdDataType.LEFT ) );
        expectedValue.add( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.2, 0.3 ),
                                                Operator.BETWEEN,
                                                ThresholdConstants.ThresholdDataType.LEFT ) );


        assertTrue( actualValue.equals( expectedValue ) );

        // Test exception    
        exception.expectCause( CoreMatchers.isA( MetricConfigException.class ) );
        method.invoke( null, new Object[] { dataFac, "0.1",
                                            Operator.BETWEEN,
                                            ThresholdConstants.ThresholdDataType.LEFT,
                                            false,
                                            null } );
    }

    /**
     * Tests the {@link MetricConfigHelper#hasSummaryStatisticsFor(ProjectConfig, java.util.function.Predicate)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testHasSummaryStatisticsForNamedMetric() throws MetricConfigException
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
    public void testHasSummaryStatisticsForAllValid() throws MetricConfigException
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
    public void testHasSummaryStatisticsThrowsExceptionOnAllValid() throws MetricConfigException
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
     * wres.datamodel.MetricConstants.MetricOutputGroup)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testHasTheseOutputsByThresholdLead() throws MetricConfigException
    {
        // Outputs by threshold and lead time required
        assertTrue( MetricConfigHelper.hasTheseOutputsByThresholdLead( defaultMockedConfig,
                                                                       MetricOutputGroup.DOUBLE_SCORE ) );

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
                                                                        MetricOutputGroup.DOUBLE_SCORE ) );

        // Output configuration defined, but is not by threshold then lead
        ProjectConfig mockedConfigWithOutput =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( null,
                                                                                      OutputTypeSelection.LEAD_THRESHOLD,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ) ),
                                   null,
                                   null );

        assertFalse( MetricConfigHelper.hasTheseOutputsByThresholdLead( mockedConfigWithOutput,
                                                                        MetricOutputGroup.DOUBLE_SCORE ) );

        // No output and no output groups of the specified type
        assertFalse( MetricConfigHelper.hasTheseOutputsByThresholdLead( mockedConfig,
                                                                        MetricOutputGroup.MULTIVECTOR ) );

    }

    /**
     * Tests the {@link MetricConfigHelper#getCacheListFromProjectConfig(ProjectConfig)}.
     * @throws MetricConfigException if the metric configuration is invalid
     */

    @Test
    public void testGetCachedListFromProjectConfig() throws MetricConfigException
    {
        // No outputs configuration defined
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.RELIABILITY_DIAGRAM ) );

        // Output configuration defined, but is not by threshold then lead
        ProjectConfig mockedConfigWithOutput =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( null,
                                                                                      OutputTypeSelection.THRESHOLD_LEAD,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ) ),
                                   null,
                                   null );

        Set<MetricOutputGroup> expected = new HashSet<>();
        expected.add( MetricOutputGroup.MULTIVECTOR );
        expected.add( MetricOutputGroup.PAIRED );
        expected.add( MetricOutputGroup.DOUBLE_SCORE );

        assertTrue( MetricConfigHelper.getCacheListFromProjectConfig( mockedConfigWithOutput ).equals( expected ) );

    }

}

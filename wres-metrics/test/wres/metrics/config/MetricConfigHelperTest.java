package wres.metrics.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
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

import org.junit.Before;
import org.junit.Test;

import wres.config.MetricConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.datamodel.thresholds.ThresholdsByMetric;

/**
 * Tests the {@link MetricConfigHelper}.
 * 
 * @author James Brown
 */
public final class MetricConfigHelperTest
{

    /**
     * Test thresholds.
     */

    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

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
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        defaultMockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
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
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> MetricConfigHelper.from( (MetricConfigName) null ) );

        assertEquals( "Specify input configuration with a non-null name to map.", actual.getMessage() );
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
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> MetricConfigHelper.from( (SummaryStatisticsName) null ) );

        assertEquals( "Specify input configuration with a non-null name to map.", actual.getMessage() );
    }

    /**
     * Tests the {@link MetricConfigHelper#getMetricsFromConfig(wres.config.generated.ProjectConfig)} by comparing
     * actual results to expected results.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @Test
    public void testGetMetricsFromConfig()
    {
        assertEquals( Set.of( MetricConstants.BIAS_FRACTION,
                              MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                              MetricConstants.MEAN_ERROR ),
                      MetricConfigHelper.getMetricsFromConfig( this.defaultMockedConfig ) );
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
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );

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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Mock external thresholds
        Map<MetricConstants, Set<ThresholdOuter>> mockExternal = new EnumMap<>( MetricConstants.class );
        Set<ThresholdOuter> atomicExternal = new HashSet<>();
        atomicExternal.add( ThresholdOuter.of( OneOrTwoDoubles.of( 0.3 ),
                                               Operator.GREATER,
                                               ThresholdConstants.ThresholdDataType.LEFT,
                                               dimension ) );
        mockExternal.put( MetricConstants.BIAS_FRACTION, atomicExternal );

        ThresholdsByMetric.Builder builder = new ThresholdsByMetric.Builder();
        builder.addThresholds( mockExternal, ThresholdGroup.VALUE );
        builder.addThresholds( ThresholdsGenerator.getThresholdsFromConfig( mockedConfig )
                                                  .getOneOrTwoThresholds() );

        ThresholdsByMetric actualByMetric = builder.build();

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new EnumMap<>( MetricConstants.class );
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1 ),
                                                                        Operator.GREATER,
                                                                        ThresholdConstants.ThresholdDataType.LEFT,
                                                                        dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                        Operator.GREATER,
                                                                        ThresholdConstants.ThresholdDataType.LEFT,
                                                                        dimension ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.3 ),
                                                                        Operator.GREATER,
                                                                        ThresholdConstants.ThresholdDataType.LEFT,
                                                                        dimension ) ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholds );

        assertEquals( expected, actual );
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
        Set<ThresholdOuter> actual = (Set<ThresholdOuter>) method.invoke( null,
                                                                          TEST_THRESHOLDS,
                                                                          Operator.BETWEEN,
                                                                          ThresholdConstants.ThresholdDataType.LEFT,
                                                                          true,
                                                                          null );

        Set<ThresholdOuter> expected = new HashSet<>();
        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1, 0.2 ),
                                                             Operator.BETWEEN,
                                                             ThresholdConstants.ThresholdDataType.LEFT ) );
        expected.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2, 0.3 ),
                                                             Operator.BETWEEN,
                                                             ThresholdConstants.ThresholdDataType.LEFT ) );
        assertEquals( expected, actual );

        // Test with value thresholds
        @SuppressWarnings( "unchecked" )
        Set<ThresholdOuter> actualValue = (Set<ThresholdOuter>) method.invoke( null,
                                                                               TEST_THRESHOLDS,
                                                                               Operator.BETWEEN,
                                                                               ThresholdConstants.ThresholdDataType.LEFT,
                                                                               false,
                                                                               null );

        Set<ThresholdOuter> expectedValue = new HashSet<>();
        expectedValue.add( ThresholdOuter.of( OneOrTwoDoubles.of( 0.1, 0.2 ),
                                              Operator.BETWEEN,
                                              ThresholdConstants.ThresholdDataType.LEFT ) );
        expectedValue.add( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2, 0.3 ),
                                              Operator.BETWEEN,
                                              ThresholdConstants.ThresholdDataType.LEFT ) );


        assertEquals( expectedValue, actualValue );

        // Test exception 
        InvocationTargetException exception = assertThrows( InvocationTargetException.class,
                                                            () -> method.invoke( null,
                                                                                 "0.1",
                                                                                 Operator.BETWEEN,
                                                                                 ThresholdConstants.ThresholdDataType.LEFT,
                                                                                 false,
                                                                                 null ) );
        assertEquals( exception.getCause().getClass(), MetricConfigException.class );
    }

    /**
     * Tests the {@link MetricConfigHelper#hasTheseOutputsByThresholdLead(ProjectConfig, 
     * wres.datamodel.metrics.MetricConstants.StatisticType)}.
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
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        assertFalse( MetricConfigHelper.hasTheseOutputsByThresholdLead( mockedConfig,
                                                                        StatisticType.DOUBLE_SCORE ) );

        // Output configuration defined, but is not by threshold then lead
        ProjectConfig mockedConfigWithOutput =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, 0, metrics, null ) ),
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
                                                                        StatisticType.DIAGRAM ) );

    }

}
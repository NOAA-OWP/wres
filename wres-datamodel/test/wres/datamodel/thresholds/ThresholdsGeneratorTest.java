package wres.datamodel.thresholds;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import wres.config.xml.MetricConfigException;
import wres.config.xml.MetricConstantsFactory;
import wres.config.generated.DestinationConfig;
import wres.config.generated.EnsembleAverageType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants.Operator;

/**
 * Tests the {@link ThresholdsGenerator}.
 * 
 * @author James Brown
 */
class ThresholdsGeneratorTest
{

    /**
     * Test thresholds.
     */

    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

    @Test
    void testGetThresholdsFromConfig()
    {
        List<MetricConfig> testMetrics = new ArrayList<>();
        testMetrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        testMetrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        testMetrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );

        // Mock some thresholds
        List<ThresholdsConfig> testThresholds = new ArrayList<>();
        testThresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                  ThresholdDataType.LEFT,
                                                  ThresholdsGeneratorTest.TEST_THRESHOLDS,
                                                  ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   List.of( new MetricsConfig( testThresholds,
                                                               0,
                                                               testMetrics,
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

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = ThresholdsGenerator.getThresholdsFromConfig( mockedConfig );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual =
                ThresholdSlicer.getOneOrTwoThresholds( actualByMetric );

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new EnumMap<>( MetricConstants.class );
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdConstants.ThresholdDataType.LEFT ) ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholds );
        expected.put( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, atomicThresholds );
        expected.put( MetricConstants.MEAN_ERROR, atomicThresholds );

        assertEquals( expected, actual );

        // Check for the same result when specifying an explicit pair configuration with null dimension
        // Create some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );

        // Create some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              ThresholdsGeneratorTest.TEST_THRESHOLDS,
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   List.of( new MetricsConfig( thresholds,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
                                   null,
                                   null,
                                   null );

        // Compute combined thresholds
        ThresholdsByMetric actualByMetricNullDimension =
                ThresholdsGenerator.getThresholdsFromConfig( mockedConfigWithNullDimension );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actualWithNullDim =
                ThresholdSlicer.getOneOrTwoThresholds( actualByMetricNullDimension );

        assertEquals( expected, actualWithNullDim );
    }

    @Test
    void testGetThresholdsFromConfigIncludesDichotomousScoresWithoutDecisionThresholds()
    {
        List<MetricConfig> testMetrics = new ArrayList<>();
        testMetrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        testMetrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        testMetrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        testMetrics.add( new MetricConfig( null, MetricConfigName.PROBABILITY_OF_DETECTION ) );

        // Mock some thresholds
        List<ThresholdsConfig> testThresholds = new ArrayList<>();
        testThresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                                  ThresholdDataType.LEFT,
                                                  ThresholdsGeneratorTest.TEST_THRESHOLDS,
                                                  ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   List.of( new MetricsConfig( testThresholds,
                                                               0,
                                                               testMetrics,
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

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = ThresholdsGenerator.getThresholdsFromConfig( mockedConfig );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual =
                ThresholdSlicer.getOneOrTwoThresholds( actualByMetric );

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new EnumMap<>( MetricConstants.class );
        Set<OneOrTwoThresholds> atomicThresholds = new HashSet<>();

        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.2 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdConstants.ThresholdDataType.LEFT ) ) );
        atomicThresholds.add( OneOrTwoThresholds.of( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.3 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdConstants.ThresholdDataType.LEFT ) ) );

        Set<OneOrTwoThresholds> atomicThresholdsWithAllData = new HashSet<>( atomicThresholds );
        atomicThresholdsWithAllData.add( OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA ) );

        expected.put( MetricConstants.BIAS_FRACTION, atomicThresholdsWithAllData );
        expected.put( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, atomicThresholdsWithAllData );
        expected.put( MetricConstants.MEAN_ERROR, atomicThresholdsWithAllData );
        expected.put( MetricConstants.PROBABILITY_OF_DETECTION, atomicThresholds );

        assertEquals( expected, actual );

        // Check for the same result when specifying an explicit pair configuration with null dimension
        // Create some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PROBABILITY_OF_DETECTION ) );

        // Create some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              ThresholdsGeneratorTest.TEST_THRESHOLDS,
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   List.of( new MetricsConfig( thresholds,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
                                   null,
                                   null,
                                   null );

        // Compute combined thresholds
        ThresholdsByMetric actualByMetricNullDimension =
                ThresholdsGenerator.getThresholdsFromConfig( mockedConfigWithNullDimension );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actualWithNullDim =
                ThresholdSlicer.getOneOrTwoThresholds( actualByMetricNullDimension );

        assertEquals( expected, actualWithNullDim );
    }

    @Test
    void testGetThresholdsFromConfigWhenNoThresholdsPresent()
    {
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );

        ProjectConfig mockedConfigWithoutThresholds =
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   List.of( new MetricsConfig( null,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
                                   null,
                                   null,
                                   null );

        // All data threshold only
        ThresholdsByMetric actualWrapped =
                ThresholdsGenerator.getThresholdsFromConfig( mockedConfigWithoutThresholds );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual =
                ThresholdSlicer.getOneOrTwoThresholds( actualWrapped );

        // Derive expected thresholds
        Map<MetricConstants, Set<OneOrTwoThresholds>> expected = new EnumMap<>( MetricConstants.class );
        Set<OneOrTwoThresholds> atomicExpected = new HashSet<>();

        atomicExpected.add( OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                      Operator.GREATER,
                                                                      ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT ) ) );
        expected.put( MetricConstants.BIAS_FRACTION, atomicExpected );

        assertEquals( expected, actual );
    }

    @Test
    void testGetThresholdOperator()
    {
        ThresholdsConfig first = new ThresholdsConfig( null,
                                                       null,
                                                       null,
                                                       ThresholdOperator.GREATER_THAN );
        assertSame( Operator.GREATER, ThresholdsGenerator.getThresholdOperator( first ) );

        ThresholdsConfig second = new ThresholdsConfig( null,
                                                        null,
                                                        null,
                                                        ThresholdOperator.LESS_THAN );
        assertSame( Operator.LESS, ThresholdsGenerator.getThresholdOperator( second ) );

        ThresholdsConfig third = new ThresholdsConfig( null,
                                                       null,
                                                       null,
                                                       ThresholdOperator.GREATER_THAN_OR_EQUAL_TO );
        assertSame( Operator.GREATER_EQUAL, ThresholdsGenerator.getThresholdOperator( third ) );

        ThresholdsConfig fourth = new ThresholdsConfig( null,
                                                        null,
                                                        null,
                                                        ThresholdOperator.LESS_THAN_OR_EQUAL_TO );
        assertSame( Operator.LESS_EQUAL, ThresholdsGenerator.getThresholdOperator( fourth ) );

        //Test exception cases
        assertThrows( NullPointerException.class,
                      () -> ThresholdsGenerator.getThresholdOperator( null ) );
        assertThrows( NullPointerException.class,
                      () -> ThresholdsGenerator.getThresholdOperator( new ThresholdsConfig( null,
                                                                                            null,
                                                                                            null,
                                                                                            null ) ) );
    }

    /**
     * Tests the {@link ThresholdsGenerator#getThresholdDataType(wres.config.generated.ThresholdDataType)}.
     */

    @Test
    void testGetThresholdDataType()
    {
        // Check that a mapping exists in the data model ThresholdDataType for all entries in the 
        // config ThresholdDataType
        for ( wres.config.generated.ThresholdDataType next : wres.config.generated.ThresholdDataType.values() )
        {
            assertNotNull( ThresholdsGenerator.getThresholdDataType( next ) );
        }
    }

    /**
     * Tests the {@link ThresholdsGenerator#getThresholdDataType(wres.config.generated.ThresholdDataType)} throws an 
     * expected exception when the input is null.
     */

    @Test
    void testGetThresholdDataTypeThrowsNPEWhenInputIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> ThresholdsGenerator.getThresholdDataType( null ) );
    }

    /**
     * Tests the {@link ThresholdsGenerator#getThresholdGroup(wres.config.generated.ThresholdType)}.
     */

    @Test
    void testGetThresholdGroup()
    {
        // Check that a mapping exists in ThresholdGroup for all entries in the ThresholdType
        for ( ThresholdType next : ThresholdType.values() )
        {
            assertNotNull( ThresholdsGenerator.getThresholdGroup( next ) );
        }
    }

    /**
     * Tests the {@link ThresholdsGenerator#getThresholdDataType(wres.config.generated.ThresholdDataType)} throws an 
     * expected exception when the input is null.
     */

    @Test
    void testGetThresholdGroupThrowsNPEWhenInputIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> ThresholdsGenerator.getThresholdGroup( null ) );
    }

    /**
     * Tests the {@link ThresholdsGenerator#getThresholdsFromConfig(ProjectConfig)} by comparing actual results to
     * expected results for a scenario where external thresholds are defined, together with a dimension for value
     * thresholds.
     * @throws MetricConfigException if an unexpected exception is encountered
     */

    @org.junit.jupiter.api.Test
    void testGetThresholdsFromConfigWithExternalThresholdsAndDimension()
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
                                                   null ),
                                   List.of( new MetricsConfig( thresholds,
                                                               0,
                                                               metrics,
                                                               null,
                                                               EnsembleAverageType.MEAN ) ),
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
        builder.addThresholds( mockExternal, ThresholdConstants.ThresholdGroup.VALUE );
        builder.addThresholds( ThresholdSlicer.getOneOrTwoThresholds( ThresholdsGenerator.getThresholdsFromConfig( mockedConfig ) ) );

        ThresholdsByMetric actualByMetric = builder.build();

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual =
                ThresholdSlicer.getOneOrTwoThresholds( actualByMetric );

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
     * Tests a method with private scope in {@link MetricConstantsFactory} using thresholds with a
     * {@link Operator#BETWEEN} condition.
     * @throws MetricConfigException if an unexpected exception is encountered
     * @throws SecurityException if the reflection fails via a security manager
     * @throws NoSuchMethodException if a matching method is not found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalArgumentException if the instance method could not be invoked with the supplied arguments
     * @throws IllegalAccessException if this Method object is enforcing Java language access control and the
     *            underlying method is inaccessible.
     */

    @org.junit.jupiter.api.Test
    void testGetThresholdsFromConfigWithBetweenCondition() throws NoSuchMethodException,
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
}

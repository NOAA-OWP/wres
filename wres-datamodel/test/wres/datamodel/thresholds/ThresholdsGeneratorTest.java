package wres.datamodel.thresholds;


import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.junit.Test;

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
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;

/**
 * Tests the {@link ThresholdsGenerator}.
 * 
 * @author James Brown
 */
public final class ThresholdsGeneratorTest
{

    /**
     * Test thresholds.
     */

    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

    @Test
    public void testGetThresholdsFromConfig()
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
                                   Arrays.asList( new MetricsConfig( testThresholds,
                                                                     0,
                                                                     testMetrics,
                                                                     null,
                                                                     EnsembleAverageType.MEAN ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( OutputTypeSelection.THRESHOLD_LEAD,
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
                                   Arrays.asList( new MetricsConfig( thresholds,
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
    public void testGetThresholdsFromConfigIncludesDichotomousScoresWithoutDecisionThresholds()
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
                                   Arrays.asList( new MetricsConfig( testThresholds,
                                                                     0,
                                                                     testMetrics,
                                                                     null,
                                                                     EnsembleAverageType.MEAN ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( OutputTypeSelection.THRESHOLD_LEAD,
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
                                   Arrays.asList( new MetricsConfig( thresholds,
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
    public void testGetThresholdsFromConfigWhenNoThresholdsPresent()
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
                                   Arrays.asList( new MetricsConfig( null,
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
}

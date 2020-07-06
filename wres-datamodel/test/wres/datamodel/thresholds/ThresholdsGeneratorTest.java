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

import org.junit.Before;
import org.junit.Test;

import wres.config.generated.DestinationConfig;
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
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdsByMetric;

/**
 * Tests the {@link ThresholdGenerator}. 
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ThresholdsGeneratorTest
{

    /**
     * Test thresholds.
     */

    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

    /**
     * Default mocked project configuration.
     */

    private ProjectConfig defaultMockedConfig;

    @Before
    public void runBeforeEachTest()
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
                                              ThresholdsGeneratorTest.TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        this.defaultMockedConfig =
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

    @Test
    public void testGetThresholdsFromConfig()
    {

        // Compute combined thresholds
        ThresholdsByMetric actualByMetric = ThresholdsGenerator.getThresholdsFromConfig( this.defaultMockedConfig );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual = actualByMetric.getOneOrTwoThresholds();

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
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Compute combined thresholds
        ThresholdsByMetric actualByMetricNullDimension =
                ThresholdsGenerator.getThresholdsFromConfig( mockedConfigWithNullDimension );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actualWithNullDim =
                actualByMetricNullDimension.getOneOrTwoThresholds();

        assertEquals( expected, actualWithNullDim );
    }

    @Test
    public void testGetThresholdsFromConfigWhenNoThresholdsPresent()
    {
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );

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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // All data threshold only
        ThresholdsByMetric actualWrapped =
                ThresholdsGenerator.getThresholdsFromConfig( mockedConfigWithoutThresholds );

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> actual = actualWrapped.getOneOrTwoThresholds();

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

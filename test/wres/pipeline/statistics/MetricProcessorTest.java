package wres.pipeline.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link MetricProcessor}.
 * 
 * @author James Brown
 */
public final class MetricProcessorTest
{

    private static final String DRRC2 = "DRRC2";

    /**
     * Threshold executor service.
     */

    private ExecutorService thresholdExecutor;

    /**
     * Metric executor service.
     */

    private ExecutorService metricExecutor;

    @Before
    public void setupBeforeEachTest()
    {
        this.thresholdExecutor = Executors.newSingleThreadExecutor();
        this.metricExecutor = Executors.newSingleThreadExecutor();
    }

    @Test
    public void testHasMetricsForMetricInputGroup() throws MetricParameterException, IOException
    {
        ProjectConfig config = TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorTest.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED ) );
    }

    @Test
    public void testHasMetricsForMetricOutputGroup() throws MetricParameterException, IOException
    {
        ProjectConfig config = TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorTest.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testHasMetricsForMetricInputGroupAndMetricOutputGroup() throws MetricParameterException, IOException
    {
        ProjectConfig config = TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorTest.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testDisallowNonScoresWithSingleValuedInput()
            throws IOException, MetricParameterException
    {
        ProjectConfig config = TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorTest.ofMetricProcessorForSingleValuedPairs( config );

        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + StatisticType.DOUBLE_SCORE
                    + "'.",
                    processor.hasMetrics( StatisticType.DOUBLE_SCORE ) );

        //Check that no non-score metrics are defined
        for ( StatisticType next : StatisticType.values() )
        {
            if ( next != StatisticType.DOUBLE_SCORE )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processor.hasMetrics( next ) );
            }
        }

    }

    @Test
    public void testDisallowNonScoresForEnsembleInput()
            throws IOException, MetricParameterException
    {
        ProjectConfig configEnsemble =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( configEnsemble );
        FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( DRRC2 ), FeatureKey.of( DRRC2 ), null );
        Map<FeatureTuple, ThresholdsByMetric> thresholds = Map.of( featureTuple, thresholdsByMetric );
        ThresholdsByMetricAndFeature metrics = ThresholdsByMetricAndFeature.of( thresholds, 0 );

        MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processorEnsemble =
                new MetricProcessorByTimeEnsemblePairs( metrics,
                                                        ForkJoinPool.commonPool(),
                                                        ForkJoinPool.commonPool() );
        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + StatisticType.DOUBLE_SCORE
                    + "'.",
                    processorEnsemble.hasMetrics( StatisticType.DOUBLE_SCORE ) );
        //Check that no non-score metrics are defined
        for ( StatisticType next : StatisticType.values() )
        {
            if ( next != StatisticType.DOUBLE_SCORE )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processorEnsemble.hasMetrics( next ) );
            }
        }
    }

    @Test
    public void testGetAllDataThreshold()
            throws MetricParameterException
    {
        ProjectConfig config = new ProjectConfig( null, null, null, null, null, null );
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorTest.ofMetricProcessorForSingleValuedPairs( config );

        ThresholdOuter expected = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT_AND_RIGHT );

        assertEquals( expected, processor.getAllDataThreshold() );
    }

    @After
    public void tearDownAfterEachTest()
    {
        this.thresholdExecutor.shutdownNow();
        this.metricExecutor.shutdownNow();
    }

    /**
     * @param config project declaration
     * @return a single-valued processor instance
     */

    private static MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>
            ofMetricProcessorForSingleValuedPairs( ProjectConfig config )
    {
        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( config );
        FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( DRRC2 ), FeatureKey.of( DRRC2 ), null );
        Map<FeatureTuple, ThresholdsByMetric> thresholds = Map.of( featureTuple, thresholdsByMetric );
        ThresholdsByMetricAndFeature metrics = ThresholdsByMetricAndFeature.of( thresholds, 0 );

        return new MetricProcessorByTimeSingleValuedPairs( metrics,
                                                           ForkJoinPool.commonPool(),
                                                           ForkJoinPool.commonPool() );
    }

}

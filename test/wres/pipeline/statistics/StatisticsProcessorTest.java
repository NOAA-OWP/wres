package wres.pipeline.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.pools.Pool;
import wres.datamodel.OneOrTwoDoubles;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.TimeSeries;
import wres.metrics.MetricParameterException;

/**
 * Tests the {@link StatisticsProcessor}.
 *
 * @author James Brown
 */
public final class StatisticsProcessorTest
{
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
    public void testHasMetricsForMetricInputGroup() throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        //Check for existence of metrics
        assertTrue( processors.stream()
                              .anyMatch( next -> next.hasMetrics( SampleDataGroup.SINGLE_VALUED ) ) );
    }

    @Test
    public void testHasMetricsForMetricOutputGroup() throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );
        //Check for existence of metrics
        assertTrue( processors.stream().anyMatch( StatisticsProcessor::hasDoubleScoreMetrics ) );
    }

    @Test
    public void testHasMetricsForMetricInputGroupAndMetricOutputGroup() throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processor =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );
        //Check for existence of metrics
        assertTrue( processor.stream()
                             .anyMatch( next -> next.hasMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                 StatisticType.DOUBLE_SCORE ) ) );
    }

    @Test
    public void testGetAllDataThreshold()
            throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ) );

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                                  .thresholds( Set.of() )
                                                                                  .metrics( metrics )
                                                                                  .build();
        evaluationDeclaration = DeclarationInterpolator.interpolate( evaluationDeclaration );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( evaluationDeclaration );

        ThresholdOuter expected = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     ThresholdOperator.GREATER,
                                                     ThresholdOrientation.LEFT_AND_RIGHT );

        assertEquals( expected, processors.get( 0 )
                                          .getAllDataThreshold() );
    }

    @After
    public void tearDownAfterEachTest()
    {
        this.thresholdExecutor.shutdownNow();
        this.metricExecutor.shutdownNow();
    }

    /**
     * @param declaration the declaration
     * @return the processors
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>>
    ofMetricProcessorForSingleValuedPairs( EvaluationDeclaration declaration )
    {
        Set<MetricsAndThresholds> metricsAndThresholdsSet =
                ThresholdSlicer.getMetricsAndThresholdsForProcessing( declaration, Set.of()  );
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors = new ArrayList<>();
        for ( MetricsAndThresholds metricsAndThresholds : metricsAndThresholdsSet )
        {
            StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor
                    = new SingleValuedStatisticsProcessor( metricsAndThresholds,
                                                           ForkJoinPool.commonPool(),
                                                           ForkJoinPool.commonPool() );
            processors.add( processor );
        }

        return Collections.unmodifiableList( processors );
    }

}

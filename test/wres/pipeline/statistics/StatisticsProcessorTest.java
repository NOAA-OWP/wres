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
import wres.datamodel.pools.Pool;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.datamodel.time.TimeSeries;
import wres.metrics.MetricParameterException;
import wres.statistics.generated.GeometryTuple;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link StatisticsProcessor}.
 * 
 * @author James Brown
 */
public final class StatisticsProcessorTest
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
        ProjectConfig config =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED ) );
    }

    @Test
    public void testHasMetricsForMetricOutputGroup() throws MetricParameterException, IOException
    {
        ProjectConfig config =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testHasMetricsForMetricInputGroupAndMetricOutputGroup() throws MetricParameterException, IOException
    {
        ProjectConfig config =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();
        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testGetAllDataThreshold()
            throws MetricParameterException
    {
        ProjectConfig config = new ProjectConfig( null, null, null, null, null, null );
        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                StatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( config );

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

    private static StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>
            ofMetricProcessorForSingleValuedPairs( ProjectConfig config )
    {
        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( config );
        GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( MessageFactory.getGeometry( DRRC2 ),
                                                                       MessageFactory.getGeometry( DRRC2 ),
                                                                       null );
        FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );
        Map<FeatureTuple, ThresholdsByMetric> thresholds = Map.of( featureTuple, thresholdsByMetric );
        ThresholdsByMetricAndFeature metrics = ThresholdsByMetricAndFeature.of( thresholds, 0 );

        return new SingleValuedStatisticsProcessor( metrics,
                                                           ForkJoinPool.commonPool(),
                                                           ForkJoinPool.commonPool() );
    }

}

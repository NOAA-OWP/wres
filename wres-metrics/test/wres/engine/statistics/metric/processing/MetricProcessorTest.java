package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.config.ProjectConfigPlus;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link MetricProcessor}.
 * 
 * @author James Brown
 */
public final class MetricProcessorTest
{
    /**
     * Source for testing.
     */

    private static final String TEST_SOURCE = "testinput/metricProcessorTest/testAllValid.xml";

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
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED ) );
    }

    @Test
    public void testHasMetricsForMetricOutputGroup() throws MetricParameterException, IOException
    {
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testHasMetricsForMetricInputGroupAndMetricOutputGroup() throws MetricParameterException, IOException
    {
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testHasThresholdMetrics()
            throws MetricParameterException
    {

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig discreteProbability =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processorWithDiscreteProbability =
                MetricFactory.ofMetricProcessorForEnsemblePairs( discreteProbability );

        //Check for existence of metrics
        assertTrue( processorWithDiscreteProbability.hasThresholdMetrics() );

        // Check dichotomous metric
        metrics.clear();
        metrics.add( new MetricConfig( null, MetricConfigName.FREQUENCY_BIAS ) );
        ProjectConfig dichotomous =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processorWithDichotomous =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( dichotomous );

        //Check for existence of metrics
        assertTrue( processorWithDichotomous.hasThresholdMetrics() );

        // Check for single-valued metric
        metrics.clear();
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        ProjectConfig singleValued =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processorWithSingleValued =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( singleValued );

        //Check for non-existence of metrics
        assertFalse( processorWithSingleValued.hasThresholdMetrics() );

        // Check multicategory metric
        metrics.clear();
        metrics.add( new MetricConfig( null, MetricConfigName.PEIRCE_SKILL_SCORE ) );
        ProjectConfig multicategory =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processorWithMultiCat =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( multicategory );

        //Check for existence of metrics
        assertTrue( processorWithMultiCat.hasThresholdMetrics() );
    }

    @Test
    public void testDisallowNonScoresWithSingleValuedInput()
            throws IOException, MetricParameterException
    {
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/testSingleValued.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config );

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
        //Ensemble case
        String configPathEnsemble = "testinput/metricProcessorTest/testDisallowNonScores.xml";
        ProjectConfig configEnsemble = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processorEnsemble =
                MetricFactory.ofMetricProcessorForEnsemblePairs( configEnsemble );
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
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config );

        ThresholdOuter expected = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT_AND_RIGHT );

        assertTrue( expected.equals( processor.getAllDataThreshold() ) );
    }

    @After
    public void tearDownAfterEachTest()
    {
        this.thresholdExecutor.shutdownNow();
        this.metricExecutor.shutdownNow();
    }

}

package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MetricProcessor}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorTest
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
        thresholdExecutor = Executors.newSingleThreadExecutor();
        metricExecutor = Executors.newSingleThreadExecutor();
    }

    /**
     * Tests the {@link MetricProcessor#getMetricOutputTypesToCache()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     * @throws InterruptedException 
     */

    @Test
    public void testGetMetricOutputTypesToCache()
            throws IOException, MetricParameterException
    {
        String configPath = "testinput/metricProcessorTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> trueProcessor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );
        MetricProcessor<SingleValuedPairs, StatisticsForProject> falseProcessor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config, null );
        //Check for storage
        assertFalse( "Expected a metric processor that stores metric outputs.",
                     trueProcessor.getMetricOutputTypesToCache().isEmpty() );
        assertTrue( "Expected a metric processor that does not store metric outputs.",
                    falseProcessor.getMetricOutputTypesToCache().isEmpty() );
    }

    /**
     * Tests the {@link MetricProcessor#getCachedMetricOutputTypes()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     * @throws InterruptedException 
     */

    @Test
    public void testGetCachedMetricOutputTypes()
            throws IOException, MetricParameterException,
            InterruptedException
    {
        // Check empty config
        ProjectConfig emptyConfig = new ProjectConfig( null, null, null, null, null, null );

        MetricProcessor<SingleValuedPairs, StatisticsForProject> emptyProcessor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( emptyConfig, null );

        assertTrue( emptyProcessor.getCachedMetricOutputTypes().isEmpty() );

        // Check config with results
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        null,
                                                                        thresholdExecutor,
                                                                        metricExecutor,
                                                                        Collections.singleton( StatisticGroup.DOUBLE_SCORE ) );
        // Compute the resuults and check the cache       
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();

        processor.apply( pairs );

        Set<StatisticGroup> expectedCache = new HashSet<>( Arrays.asList( StatisticGroup.DOUBLE_SCORE ) );

        assertTrue( expectedCache.equals( processor.getCachedMetricOutputTypes() ) );
    }

    /**
     * Tests {@link MetricProcessor#hasMetrics(SampleDataGroup)}.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testHasMetricsForMetricInputGroup() throws MetricParameterException, IOException
    {
        String configPath = "testinput/metricProcessorTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED ) );
    }

    /**
     * Tests {@link MetricProcessor#hasMetrics(StatisticGroup)}.
     * 
     * @throws IOException if the input data could not be read
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testHasMetricsForMetricOutputGroup() throws MetricParameterException, IOException
    {
        String configPath = "testinput/metricProcessorTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( StatisticGroup.DOUBLE_SCORE ) );
    }

    /**
     * Tests {@link MetricProcessor#hasMetrics(SampleDataGroup, StatisticGroup)}.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testHasMetricsForMetricInputGroupAndMetricOutputGroup() throws MetricParameterException, IOException
    {
        String configPath = "testinput/metricProcessorTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ) );
    }


    /**
     * Tests {@link MetricProcessor#hasThresholdMetrics()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testHasThresholdMetrics()
            throws MetricParameterException
    {

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE ) );

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
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<EnsemblePairs, StatisticsForProject> processorWithDiscreteProbability =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( discreteProbability,
                                                                    StatisticGroup.set() );

        //Check for existence of metrics
        assertTrue( processorWithDiscreteProbability.hasThresholdMetrics() );

        // Check dichotomous metric
        metrics.clear();
        metrics.add( new MetricConfig( null, null, MetricConfigName.FREQUENCY_BIAS ) );
        ProjectConfig dichotomous =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<SingleValuedPairs, StatisticsForProject> processorWithDichotomous =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( dichotomous,
                                                                        StatisticGroup.set() );

        //Check for existence of metrics
        assertTrue( processorWithDichotomous.hasThresholdMetrics() );

        // Check for single-valued metric
        metrics.clear();
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );
        ProjectConfig singleValued =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<SingleValuedPairs, StatisticsForProject> processorWithSingleValued =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( singleValued,
                                                                        StatisticGroup.set() );

        //Check for non-existence of metrics
        assertFalse( processorWithSingleValued.hasThresholdMetrics() );

        // Check multicategory metric
        metrics.clear();
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEIRCE_SKILL_SCORE ) );
        ProjectConfig multicategory =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<SingleValuedPairs, StatisticsForProject> processorWithMultiCat =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( multicategory,
                                                                        StatisticGroup.set() );

        //Check for existence of metrics
        assertTrue( processorWithMultiCat.hasThresholdMetrics() );
    }

    /**
     * Tests that non-score metrics are disallowed when configuring "all valid" metrics in combination with 
     * {@link PairConfig#getIssuedDatesPoolingWindow()} that is not null. Uses the configuration in 
     * testinput/metricProcessorTest/testSingleValued.xml for single-valued input.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testDisallowNonScoresWithSingleValuedInput()
            throws IOException, MetricParameterException
    {
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/testSingleValued.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );

        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + StatisticGroup.DOUBLE_SCORE
                    + "'.",
                    processor.hasMetrics( StatisticGroup.DOUBLE_SCORE ) );

        //Check that no non-score metrics are defined
        for ( StatisticGroup next : StatisticGroup.values() )
        {
            if ( next != StatisticGroup.DOUBLE_SCORE )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processor.hasMetrics( next ) );
            }
        }

    }

    /**
     * Tests that non-score metrics are disallowed when configuring "all valid" metrics in combination with 
     * {@link PairConfig#getIssuedDatesPoolingWindow()} that is not null. Uses the configuration in 
     * testinput/metricProcessorTest/testDisallowNonScores.xml for ensemble input.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testDisallowNonScoresForEnsembleInput()
            throws IOException, MetricParameterException
    {
        //Ensemble case
        String configPathEnsemble = "testinput/metricProcessorTest/testDisallowNonScores.xml";
        ProjectConfig configEnsemble = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, StatisticsForProject> processorEnsemble =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( configEnsemble,
                                                                    StatisticGroup.set() );
        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + StatisticGroup.DOUBLE_SCORE
                    + "'.",
                    processorEnsemble.hasMetrics( StatisticGroup.DOUBLE_SCORE ) );
        //Check that no non-score metrics are defined
        for ( StatisticGroup next : StatisticGroup.values() )
        {
            if ( next != StatisticGroup.DOUBLE_SCORE )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processorEnsemble.hasMetrics( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricProcessor#doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.SampleDataGroup, 
     * wres.datamodel.MetricConstants.StatisticGroup, wres.datamodel.thresholds.Threshold)}. 
     * Uses the configuration in testinput/metricProcessorTest/testDoNotComputeTheseMetricsForThisThreshold.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testDoNotComputeTheseMetricsForThisThresholdWithSingleValuedInput()
            throws IOException, MetricParameterException
    {
        //Single-valued case
        String configPathSingleValued =
                "testinput/metricProcessorTest/testDoNotComputeTheseMetricsForThisThreshold.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );

        ThresholdsByMetric thresholds =
                processor.getThresholdsByMetric().filterByGroup( SampleDataGroup.SINGLE_VALUED,
                                                                 StatisticGroup.DOUBLE_SCORE );

        Threshold firstTest =
                Threshold.of( OneOrTwoDoubles.of( 0.5 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + firstTest
                    + "'",
                    firstSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR ) ) ) );
        Threshold secondTest =
                Threshold.of( OneOrTwoDoubles.of( 0.75 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + secondTest
                    + "'",
                    secondSet.equals( new HashSet<>( Arrays.asList() ) ) );
        Threshold thirdTest =
                Threshold.of( OneOrTwoDoubles.of( 0.83 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + thirdTest
                    + "'",
                    thirdSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR ) ) ) );
        Threshold fourthTest =
                Threshold.of( OneOrTwoDoubles.of( 0.9 ),
                                         Operator.GREATER,
                                         ThresholdDataType.LEFT,
                                         MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + fourthTest
                    + "'",
                    fourthSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                    MetricConstants.ROOT_MEAN_SQUARE_ERROR ) ) ) );
    }

    /**
     * Tests the {@link MetricProcessor#doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.SampleDataGroup, 
     * wres.datamodel.MetricConstants.StatisticGroup, wres.datamodel.thresholds.Threshold)}. 
     * Uses the configuration in testinput/metricProcessorTest/testEnsemble.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testDoNotComputeTheseMetricsForThisThresholdWithEnsembleInput()
            throws IOException, MetricParameterException
    {
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/testEnsemble.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    StatisticGroup.set() );
        Threshold firstTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                  Operator.GREATER,
                                                                  ThresholdDataType.LEFT );

        ThresholdsByMetric thresholds = processor.getThresholdsByMetric();

        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );

        assertTrue( "Unexpected set of metrics to ignore for threshold '" + firstTest
                    + "'",
                    firstSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                   MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.BRIER_SCORE ) ) ) );

        Threshold secondTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.25 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + secondTest
                    + "'",
                    secondSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.MEAN_SQUARE_ERROR,
                                                                    MetricConstants.BRIER_SCORE ) ) ) );

        Threshold thirdTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                  Operator.GREATER,
                                                                  ThresholdDataType.LEFT );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );

        assertTrue( "Unexpected set of metrics to ignore for threshold '" + thirdTest
                    + "'",
                    thirdSet.equals( new HashSet<>( Arrays.asList( MetricConstants.BRIER_SKILL_SCORE,
                                                                   MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) ) ) );

        Threshold fourthTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.925 ),
                                                                   Operator.GREATER,
                                                                   ThresholdDataType.LEFT );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + fourthTest
                    + "'",
                    fourthSet.equals( new HashSet<>( Arrays.asList() ) ) );
    }

    /**
     * Tests the {@link MetricProcessor#getAllDataThreshold()}.
     * 
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testGetAllDataThreshold()
            throws MetricParameterException
    {
        ProjectConfig config = new ProjectConfig( null, null, null, null, null, null );
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );

        Threshold expected = Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                      Operator.GREATER,
                                                      ThresholdDataType.LEFT_AND_RIGHT );

        assertTrue( expected.equals( processor.getAllDataThreshold() ) );
    }

    @After
    public void tearDownAfterEachTest()
    {
        thresholdExecutor.shutdownNow();
        metricExecutor.shutdownNow();
    }

}

package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;
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

import wres.config.ProjectConfigPlus;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
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
        thresholdExecutor = Executors.newSingleThreadExecutor();
        metricExecutor = Executors.newSingleThreadExecutor();
    }

    @Test
    public void testGetMetricOutputTypesToCache()
            throws IOException, MetricParameterException
    {
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> trueProcessor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );
        MetricProcessor<PoolOfPairs<Double, Double>> falseProcessor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config, null );
        //Check for storage
        assertFalse( trueProcessor.getMetricOutputTypesToCache().isEmpty() );
        assertTrue( falseProcessor.getMetricOutputTypesToCache().isEmpty() );
    }

    @Test
    public void testGetCachedMetricOutputTypes()
            throws IOException, MetricParameterException,
            InterruptedException
    {
        // Check empty config
        ProjectConfig emptyConfig = new ProjectConfig( null, null, null, null, null, null );

        MetricProcessor<PoolOfPairs<Double, Double>> emptyProcessor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( emptyConfig, null );

        assertTrue( emptyProcessor.getCachedMetricOutputTypes().isEmpty() );

        // Check config with results
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        null,
                                                                        thresholdExecutor,
                                                                        metricExecutor,
                                                                        Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        // Compute the resuults and check the cache       
        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsFour();

        processor.apply( pairs );

        Set<StatisticType> expectedCache = new HashSet<>( Arrays.asList( StatisticType.DOUBLE_SCORE ) );

        assertTrue( expectedCache.equals( processor.getCachedMetricOutputTypes() ) );
    }

    @Test
    public void testHasMetricsForMetricInputGroup() throws MetricParameterException, IOException
    {
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED ) );
    }

    @Test
    public void testHasMetricsForMetricOutputGroup() throws MetricParameterException, IOException
    {
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( StatisticType.DOUBLE_SCORE ) );
    }

    @Test
    public void testHasMetricsForMetricInputGroupAndMetricOutputGroup() throws MetricParameterException, IOException
    {
        String configPath = TEST_SOURCE;
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );
    }

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


        MetricProcessor<PoolOfPairs<Double, Ensemble>> processorWithDiscreteProbability =
                MetricFactory.ofMetricProcessorForEnsemblePairs( discreteProbability,
                                                                    StatisticType.set() );

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


        MetricProcessor<PoolOfPairs<Double, Double>> processorWithDichotomous =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( dichotomous,
                                                                        StatisticType.set() );

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


        MetricProcessor<PoolOfPairs<Double, Double>> processorWithSingleValued =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( singleValued,
                                                                        StatisticType.set() );

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


        MetricProcessor<PoolOfPairs<Double, Double>> processorWithMultiCat =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( multicategory,
                                                                        StatisticType.set() );

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
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );

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
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processorEnsemble =
                MetricFactory.ofMetricProcessorForEnsemblePairs( configEnsemble,
                                                                    StatisticType.set() );
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
    public void testDoNotComputeTheseMetricsForThisThresholdWithSingleValuedInput()
            throws IOException, MetricParameterException
    {
        //Single-valued case
        String configPathSingleValued =
                "testinput/metricProcessorTest/testDoNotComputeTheseMetricsForThisThreshold.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );

        ThresholdsByMetric thresholds =
                processor.getThresholdsByMetric()
                         .filterByGroup( SampleDataGroup.SINGLE_VALUED,
                                         StatisticType.DOUBLE_SCORE );

        Threshold firstTest =
                Threshold.of( OneOrTwoDoubles.of( 0.5 ),
                              Operator.GREATER,
                              ThresholdDataType.LEFT,
                              MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );

        assertEquals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                    MetricConstants.MEAN_ABSOLUTE_ERROR ) ),
                      firstSet );
        Threshold secondTest =
                Threshold.of( OneOrTwoDoubles.of( 0.75 ),
                              Operator.GREATER,
                              ThresholdDataType.LEFT,
                              MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );

        assertEquals( Collections.emptySet(), secondSet );
        Threshold thirdTest =
                Threshold.of( OneOrTwoDoubles.of( 0.83 ),
                              Operator.GREATER,
                              ThresholdDataType.LEFT,
                              MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );

        assertEquals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                    MetricConstants.MEAN_ABSOLUTE_ERROR ) ),
                      thirdSet );
        Threshold fourthTest =
                Threshold.of( OneOrTwoDoubles.of( 0.9 ),
                              Operator.GREATER,
                              ThresholdDataType.LEFT,
                              MeasurementUnit.of( "CMS" ) );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );

        assertEquals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                    MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                    MetricConstants.ROOT_MEAN_SQUARE_ERROR ) ),
                      fourthSet );
    }

    @Test
    public void testDoNotComputeTheseMetricsForThisThresholdWithEnsembleInput()
            throws IOException, MetricParameterException
    {
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/testEnsemble.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                    StatisticType.set() );
        Threshold firstTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.1 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT );

        ThresholdsByMetric thresholds = processor.getThresholdsByMetric();

        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );

        assertEquals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                    MetricConstants.MEAN_SQUARE_ERROR,
                                                    MetricConstants.BRIER_SCORE ) ),
                      firstSet );

        Threshold secondTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.25 ),
                                                                 Operator.GREATER,
                                                                 ThresholdDataType.LEFT );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );

        assertEquals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                    MetricConstants.MEAN_SQUARE_ERROR,
                                                    MetricConstants.BRIER_SCORE ) ),
                      secondSet );

        Threshold thirdTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );

        assertEquals( new HashSet<>( Arrays.asList( MetricConstants.BRIER_SKILL_SCORE,
                                                    MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) ),
                      thirdSet );

        Threshold fourthTest = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.925 ),
                                                                 Operator.GREATER,
                                                                 ThresholdDataType.LEFT );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );

        assertEquals( Collections.emptySet(), fourthSet );
    }

    @Test
    public void testGetAllDataThreshold()
            throws MetricParameterException
    {
        ProjectConfig config = new ProjectConfig( null, null, null, null, null, null );
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                        StatisticType.set() );

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

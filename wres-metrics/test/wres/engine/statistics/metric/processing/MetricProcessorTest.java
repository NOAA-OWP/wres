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
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
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
     * Data factory.
     */

    private DataFactory metIn;

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
        metIn = DefaultDataFactory.getInstance();
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
            throws IOException, MetricParameterException, MetricProcessorException,
            InterruptedException
    {
        String configPath = "testinput/metricProcessorTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> trueProcessor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> falseProcessor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config, null );
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
            throws IOException, MetricParameterException, MetricProcessorException,
            InterruptedException
    {
        // Check empty config
        ProjectConfig emptyConfig = new ProjectConfig( null, null, null, null, null, null );

        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> emptyProcessor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( emptyConfig, null );

        assertTrue( emptyProcessor.getCachedMetricOutputTypes().isEmpty() );

        // Check config with results
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        null,
                                                                        thresholdExecutor,
                                                                        metricExecutor,
                                                                        Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        // Compute the resuults and check the cache       
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();

        processor.apply( pairs );

        Set<MetricOutputGroup> expectedCache = new HashSet<>( Arrays.asList( MetricOutputGroup.DOUBLE_SCORE ) );

        assertTrue( expectedCache.equals( processor.getCachedMetricOutputTypes() ) );
    }

    /**
     * Tests {@link MetricProcessor#hasMetrics(MetricInputGroup)}.
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
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( MetricInputGroup.SINGLE_VALUED ) );
    }

    /**
     * Tests {@link MetricProcessor#hasMetrics(MetricOutputGroup)}.
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
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( MetricOutputGroup.DOUBLE_SCORE ) );
    }

    /**
     * Tests {@link MetricProcessor#hasMetrics(MetricInputGroup, MetricOutputGroup)}.
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
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );
        //Check for existence of metrics
        assertTrue( processor.hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ) );
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
            throws IOException, MetricParameterException
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


        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processorWithDiscreteProbability =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeEnsemblePairs( discreteProbability,
                                                                    MetricOutputGroup.set() );

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


        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processorWithDichotomous =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( dichotomous,
                                                                        MetricOutputGroup.set() );

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


        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processorWithSingleValued =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( singleValued,
                                                                        MetricOutputGroup.set() );

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


        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processorWithMultiCat =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( multicategory,
                                                                        MetricOutputGroup.set() );

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
            throws IOException, MetricParameterException, MetricProcessorException
    {
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/testSingleValued.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );

        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + MetricOutputGroup.DOUBLE_SCORE
                    + "'.",
                    processor.hasMetrics( MetricOutputGroup.DOUBLE_SCORE ) );

        //Check that no non-score metrics are defined
        for ( MetricOutputGroup next : MetricOutputGroup.values() )
        {
            if ( next != MetricOutputGroup.DOUBLE_SCORE )
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
            throws IOException, MetricParameterException, MetricProcessorException
    {
        //Ensemble case
        String configPathEnsemble = "testinput/metricProcessorTest/testDisallowNonScores.xml";
        ProjectConfig configEnsemble = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processorEnsemble =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeEnsemblePairs( configEnsemble,
                                                                    MetricOutputGroup.set() );
        //Check that score metrics are defined 
        assertTrue( "Expected metrics for '" + MetricOutputGroup.DOUBLE_SCORE
                    + "'.",
                    processorEnsemble.hasMetrics( MetricOutputGroup.DOUBLE_SCORE ) );
        //Check that no non-score metrics are defined
        for ( MetricOutputGroup next : MetricOutputGroup.values() )
        {
            if ( next != MetricOutputGroup.DOUBLE_SCORE )
            {
                assertFalse( "Did not expect metrics for '" + next
                             + "'.",
                             processorEnsemble.hasMetrics( next ) );
            }
        }
    }

    /**
     * Tests the {@link MetricProcessor#doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.MetricInputGroup, 
     * wres.datamodel.MetricConstants.MetricOutputGroup, wres.datamodel.thresholds.Threshold)}. 
     * Uses the configuration in testinput/metricProcessorTest/testDoNotComputeTheseMetricsForThisThreshold.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testDoNotComputeTheseMetricsForThisThresholdWithSingleValuedInput()
            throws IOException, MetricParameterException, MetricProcessorException
    {
        MetadataFactory metFac = metIn.getMetadataFactory();

        //Single-valued case
        String configPathSingleValued =
                "testinput/metricProcessorTest/testDoNotComputeTheseMetricsForThisThreshold.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );

        ThresholdsByMetric thresholds =
                processor.getThresholdsByMetric().filterByGroup( MetricInputGroup.SINGLE_VALUED,
                                                                 MetricOutputGroup.DOUBLE_SCORE );

        Threshold firstTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.5 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> firstSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( firstTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + firstTest
                    + "'",
                    firstSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR ) ) ) );
        Threshold secondTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.75 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + secondTest
                    + "'",
                    secondSet.equals( new HashSet<>( Arrays.asList() ) ) );
        Threshold thirdTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.83 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + thirdTest
                    + "'",
                    thirdSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR ) ) ) );
        Threshold fourthTest =
                metIn.ofThreshold( metIn.ofOneOrTwoDoubles( 0.9 ),
                                   Operator.GREATER,
                                   ThresholdDataType.LEFT,
                                   metFac.getDimension( "CMS" ) );
        Set<MetricConstants> fourthSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( fourthTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + fourthTest
                    + "'",
                    fourthSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                    MetricConstants.ROOT_MEAN_SQUARE_ERROR ) ) ) );
    }

    /**
     * Tests the {@link MetricProcessor#doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.MetricInputGroup, 
     * wres.datamodel.MetricConstants.MetricOutputGroup, wres.datamodel.thresholds.Threshold)}. 
     * Uses the configuration in testinput/metricProcessorTest/testEnsemble.xml.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws MetricParameterException if a metric parameter is incorrect
     */

    @Test
    public void testDoNotComputeTheseMetricsForThisThresholdWithEnsembleInput()
            throws IOException, MetricParameterException, MetricProcessorException
    {
        //Single-valued case
        String configPathSingleValued = "testinput/metricProcessorTest/testEnsemble.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.set() );
        Threshold firstTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.1 ),
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

        Threshold secondTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.25 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Set<MetricConstants> secondSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( secondTest );
        assertTrue( "Unexpected set of metrics to ignore for threshold '" + secondTest
                    + "'",
                    secondSet.equals( new HashSet<>( Arrays.asList( MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.MEAN_SQUARE_ERROR,
                                                                    MetricConstants.BRIER_SCORE ) ) ) );

        Threshold thirdTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.5 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Set<MetricConstants> thirdSet =
                thresholds.doesNotHaveTheseMetricsForThisThreshold( thirdTest );

        assertTrue( "Unexpected set of metrics to ignore for threshold '" + thirdTest
                    + "'",
                    thirdSet.equals( new HashSet<>( Arrays.asList( MetricConstants.BRIER_SKILL_SCORE,
                                                                   MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) ) ) );

        Threshold fourthTest = metIn.ofProbabilityThreshold( metIn.ofOneOrTwoDoubles( 0.925 ),
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
            throws MetricParameterException, MetricProcessorException
    {
        ProjectConfig config = new ProjectConfig( null, null, null, null, null, null );
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );

        Threshold expected = metIn.ofThreshold( metIn.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
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

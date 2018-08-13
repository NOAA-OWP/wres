package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MetricProcessorByTimeEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorByTimeEnsemblePairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link MetricProcessorByTimeEnsemblePairs#getFilterForEnsemblePairs(wres.datamodel.thresholds.Threshold)}.
     */

    @Test
    public void testGetFilterForEnsemblePairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                   condition,
                                                                                                   ThresholdDataType.RIGHT_MEAN ) ) );
        // Check that average works        
        EnsemblePair pair = EnsemblePair.of( 1.0, new double[] { 1.5, 2.0 } );

        assertTrue( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( Threshold.of( doubles,
                                                                                                condition,
                                                                                                ThresholdDataType.RIGHT_MEAN ) )
                                                      .test( pair ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} and application of
     * {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithoutThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the execution was interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithoutThresholds.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessorByTime<EnsemblePairs> processor = MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                                                             null );
        MetricOutputForProject results =
                processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
        DoubleScoreOutput bias =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.BIAS_FRACTION ).getData().get( 0 );
        DoubleScoreOutput cod =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.COEFFICIENT_OF_DETERMINATION )
                      .getData()
                      .get( 0 );
        DoubleScoreOutput rho =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                      .getData()
                      .get( 0 );
        DoubleScoreOutput mae =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.MEAN_ABSOLUTE_ERROR ).getData().get( 0 );
        DoubleScoreOutput me =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.MEAN_ERROR ).getData().get( 0 );
        DoubleScoreOutput rmse =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                      .getData()
                      .get( 0 );
        DoubleScoreOutput crps =
                Slicer.filter( results.getDoubleScoreOutput(), MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE )
                      .getData()
                      .get( 0 );

        //Test contents
        assertTrue( bias.getData().equals( -0.032093836077598345 ) );
        assertTrue( cod.getData().equals( 0.7873367083297588 ) );
        assertTrue( rho.getData().equals( 0.8873199582618204 ) );
        assertTrue( mae.getData().equals( 11.009512537315405 ) );
        assertTrue( me.getData().equals( -1.157869354367079 ) );
        assertTrue( rmse.getData().equals( 41.01563032408479 ) );
        assertTrue( Double.compare( crps.getData(), 9.076475676968208 ) == 0 );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} and application of
     * {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the execution was interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithValueThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Validate bias
        List<DoubleScoreOutput> bias = Slicer.filter( processor.getCachedMetricOutput()
                                                               .getDoubleScoreOutput(),
                                                      MetricConstants.BIAS_FRACTION )
                                             .getData();
        assertTrue( bias.get( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( bias.get( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( bias.get( 4 ).getData().equals( -0.0505708024162773 ) );
        assertTrue( bias.get( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        List<DoubleScoreOutput> cod = Slicer.filter( processor.getCachedMetricOutput()
                                                              .getDoubleScoreOutput(),
                                                     MetricConstants.COEFFICIENT_OF_DETERMINATION )
                                            .getData();

        assertTrue( cod.get( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( cod.get( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( cod.get( 4 ).getData().equals( 0.7542039364210298 ) );
        assertTrue( cod.get( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        List<DoubleScoreOutput> rho = Slicer.filter( processor.getCachedMetricOutput()
                                                              .getDoubleScoreOutput(),
                                                     MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                                            .getData();
        assertTrue( rho.get( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( rho.get( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( rho.get( 4 ).getData().equals( 0.868449155921652 ) );
        assertTrue( rho.get( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        List<DoubleScoreOutput> mae = Slicer.filter( processor.getCachedMetricOutput()
                                                              .getDoubleScoreOutput(),
                                                     MetricConstants.MEAN_ABSOLUTE_ERROR )
                                            .getData();
        assertTrue( mae.get( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( mae.get( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( mae.get( 4 ).getData().equals( 20.625668563442147 ) );
        assertTrue( mae.get( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        List<DoubleScoreOutput> me = Slicer.filter( processor.getCachedMetricOutput()
                                                             .getDoubleScoreOutput(),
                                                    MetricConstants.MEAN_ERROR )
                                           .getData();
        assertTrue( me.get( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( me.get( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( me.get( 4 ).getData().equals( -3.4840043925326936 ) );
        assertTrue( me.get( 5 ).getData().equals( -4.218543908073952 ) );
        //Validate rmse
        List<DoubleScoreOutput> rmse = Slicer.filter( processor.getCachedMetricOutput()
                                                               .getDoubleScoreOutput(),
                                                      MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                                             .getData();
        assertTrue( rmse.get( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 2 ).getData().equals( 52.55361580348336 ) );
        assertTrue( rmse.get( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( rmse.get( 4 ).getData().equals( 58.12352988180837 ) );
        assertTrue( rmse.get( 5 ).getData().equals( 61.12163959516186 ) );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} with configuration that contains
     * value thresholds and categorical measures.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws IOException if the pairs could not be read
     * @throws InterruptedException if the execution was interrupted
     */

    @Test
    public void testApplyWithValueThresholdsAndCategoricalMeasures()
            throws MetricParameterException, IOException, InterruptedException
    {
        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.THREAT_SCORE ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEIRCE_SKILL_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY_CLASSIFIER,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.5",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessorByTime<EnsemblePairs> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );

        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        // Obtain the results
        ListOfMetricOutput<DoubleScoreOutput> actual = processor.getCachedMetricOutput()
                                                                .getDoubleScoreOutput();

        // Check for equality
        BiPredicate<Double, Double> testEqual = FunctionFactory.doubleEquals();

        assertTrue( testEqual.test( Slicer.filter( actual, MetricConstants.THREAT_SCORE ).getData().get( 0 ).getData(),
                                    0.9160756501182034 ) );
        assertTrue( testEqual.test( Slicer.filter( actual, MetricConstants.PEIRCE_SKILL_SCORE )
                                          .getData()
                                          .get( 0 )
                                          .getData(),
                                    -0.0012886597938144284 ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} and application of
     * {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithProbabilityThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the execution was interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithProbabilityThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithProbabilityThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Obtain the results
        ListOfMetricOutput<DoubleScoreOutput> results = processor.getCachedMetricOutput()
                                                                 .getDoubleScoreOutput();

        //Validate a selection of the outputs only

        //Validate bias
        List<DoubleScoreOutput> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION ).getData();
        assertTrue( bias.get( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( bias.get( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( bias.get( 4 ).getData().equals( -0.05090288343061958 ) );
        assertTrue( bias.get( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        List<DoubleScoreOutput> cod = Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION ).getData();
        assertTrue( cod.get( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( cod.get( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( cod.get( 4 ).getData().equals( 0.7540690263086123 ) );
        assertTrue( cod.get( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        List<DoubleScoreOutput> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT ).getData();
        assertTrue( rho.get( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( rho.get( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( rho.get( 4 ).getData().equals( 0.8683714794421868 ) );
        assertTrue( rho.get( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        List<DoubleScoreOutput> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR ).getData();
        assertTrue( mae.get( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( mae.get( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( mae.get( 4 ).getData().equals( 20.653785159500924 ) );
        assertTrue( mae.get( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        List<DoubleScoreOutput> me = Slicer.filter( results, MetricConstants.MEAN_ERROR ).getData();
        assertTrue( me.get( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( me.get( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( me.get( 4 ).getData().equals( -3.5134287820490364 ) );
        assertTrue( me.get( 5 ).getData().equals( -4.218543908073952 ) );
        //Validate rmse
        List<DoubleScoreOutput> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR ).getData();
        assertTrue( rmse.get( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 2 ).getData().equals( 52.55361580348336 ) );
        assertTrue( rmse.get( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( rmse.get( 4 ).getData().equals( 58.19124412599005 ) );
        assertTrue( rmse.get( 5 ).getData().equals( 61.12163959516186 ) );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} throws an expected
     * exception on receiving null input.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Expected non-null input to the metric processor." );
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( new ProjectConfig( null,
                                                                                       null,
                                                                                       null,
                                                                                       null,
                                                                                       null,
                                                                                       null ),
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( null );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} throws an expected
     * exception when attempting to compute metrics that require thresholds and no thresholds are configured.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricParameterException, IOException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Cannot configure 'BRIER SCORE' without thresholds to define the events: correct the "
                                 + "configuration labelled 'null'." );

        MetricsConfig metrics =
                new MetricsConfig( null,
                                   Arrays.asList( new MetricConfig( null, null, MetricConfigName.BRIER_SCORE ) ),
                                   null );
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( new ProjectConfig( null,
                                                                                       null,
                                                                                       Arrays.asList( metrics ),
                                                                                       null,
                                                                                       null,
                                                                                       null ),
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} throws an expected
     * exception when climatological observations are required but missing.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricParameterException, IOException
    {
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Unable to determine quantile threshold from probability threshold: no climatological "
                                 + "observations were available in the input" );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BRIER_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsThree() );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} throws an expected
     * exception when baseline pairs are required but missing.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionWhenBaselineIsMissing()
            throws MetricParameterException, IOException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Specify a non-null baseline from which to generate the 'CONTINUOUS RANKED "
                                 + "PROBABILITY SKILL SCORE'." );

        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsThree() );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} throws an expected
     * exception when a dichotomous measure is configured, but classifier thresholds are missing.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionForDichotomousMetricWhenClassifierThresholdsAreMissing()
            throws MetricParameterException, IOException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "In order to configure dichotomous metrics for ensemble inputs, every metric group "
                                 + "that contains dichotomous metrics must also contain thresholds for classifying the forecast "
                                 + "probabilities into occurrences and non-occurrences." );

        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.THREAT_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
                                                            Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} throws an expected
     * exception when a multicategory measure is configured, but classifier thresholds are missing.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionForMulticategoryMetricWhenClassifierThresholdsAreMissing()
            throws MetricParameterException, IOException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "In order to configure multicategory metrics for ensemble inputs, every metric "
                                 + "group that contains multicategory metrics must also contain thresholds for classifying the "
                                 + "forecast probabilities into occurrences and non-occurrences." );

        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEIRCE_SKILL_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
                                                            Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} for all valid metrics associated
     * with ensemble inputs.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyWithAllValidMetrics() throws IOException, MetricParameterException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testAllValid.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.set() );
        //Check for the expected number of metrics
        //One fewer than total, as sample size appears in both ensemble and single-valued
        assertTrue( processor.metrics.size() == MetricInputGroup.ENSEMBLE.getMetrics().size()
                                                + MetricInputGroup.DISCRETE_PROBABILITY.getMetrics().size()
                                                + MetricInputGroup.SINGLE_VALUED.getMetrics().size()
                                                - 1 );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} and application of
     * {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} to configuration 
     * obtained from testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml and pairs 
     * obtained from {@link MetricTestDataFactory#getEnsemblePairsOneWithMissings()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithValueThresholdsAndMissings()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOneWithMissings() );

        //Obtain the results
        ListOfMetricOutput<DoubleScoreOutput> results = processor.getCachedMetricOutput()
                                                                 .getDoubleScoreOutput();
        //Validate bias
        List<DoubleScoreOutput> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION ).getData();

        assertTrue( bias.get( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( bias.get( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( bias.get( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( bias.get( 4 ).getData().equals( -0.0505708024162773 ) );
        assertTrue( bias.get( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        List<DoubleScoreOutput> cod = Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION ).getData();
        assertTrue( cod.get( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( cod.get( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( cod.get( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( cod.get( 4 ).getData().equals( 0.7542039364210298 ) );
        assertTrue( cod.get( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        List<DoubleScoreOutput> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT ).getData();
        assertTrue( rho.get( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( rho.get( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( rho.get( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( rho.get( 4 ).getData().equals( 0.868449155921652 ) );
        assertTrue( rho.get( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        List<DoubleScoreOutput> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR ).getData();
        assertTrue( mae.get( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( mae.get( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( mae.get( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( mae.get( 4 ).getData().equals( 20.625668563442147 ) );
        assertTrue( mae.get( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        List<DoubleScoreOutput> me = Slicer.filter( results, MetricConstants.MEAN_ERROR ).getData();
        assertTrue( me.get( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( me.get( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( me.get( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( me.get( 4 ).getData().equals( -3.4840043925326936 ) );
        assertTrue( me.get( 5 ).getData().equals( -4.218543908073952 ) );
        //Validate rmse
        List<DoubleScoreOutput> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR ).getData();
        assertTrue( rmse.get( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( rmse.get( 2 ).getData().equals( 52.55361580348336 ) );
        assertTrue( rmse.get( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( rmse.get( 4 ).getData().equals( 58.12352988180837 ) );
        assertTrue( rmse.get( 5 ).getData().equals( 61.12163959516186 ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} and application of
     * {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsByTimeTest/testContingencyTable.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsTwo()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testContingencyTable()
            throws IOException, MetricOutputAccessException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testContingencyTable.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config, MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );

        // Expected result
        final TimeWindow expectedWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( 24 ) );

        //Obtain the results
        ListOfMetricOutput<MatrixOutput> results =
                Slicer.filter( processor.getCachedMetricOutput().getMatrixOutput(),
                               meta -> meta.getMetricID().equals( MetricConstants.CONTINGENCY_TABLE )
                                       && meta.getTimeWindow().equals( expectedWindow ) );


        // Exceeds 50.0 with occurrences > 0.05
        MatrixOfDoubles expectedFirst = MatrixOfDoubles.of( new double[][] { { 40.0, 32.0 }, { 2.0, 91.0 } } );
        OneOrTwoThresholds first = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.05 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        assertEquals( expectedFirst,
                      Slicer.filter( results, meta -> meta.getThresholds().equals( first ) )
                            .getData()
                            .get( 0 )
                            .getData() );

        // Exceeds 50.0 with occurrences > 0.25
        MatrixOfDoubles expectedSecond = MatrixOfDoubles.of( new double[][] { { 39.0, 17.0 }, { 3.0, 106.0 } } );
        OneOrTwoThresholds second = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 50.0 ),
                                                                         Operator.GREATER,
                                                                         ThresholdDataType.LEFT ),
                                                           Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.25 ),
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT ) );

        assertEquals( expectedSecond,
                      Slicer.filter( results, meta -> meta.getThresholds().equals( second ) )
                            .getData()
                            .get( 0 )
                            .getData() );

        // Exceeds 50.0 with occurrences > 0.5
        MatrixOfDoubles expectedThird = MatrixOfDoubles.of( new double[][] { { 39.0, 15.0 }, { 3.0, 108.0 } } );
        OneOrTwoThresholds third = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        assertEquals( expectedThird,
                      Slicer.filter( results, meta -> meta.getThresholds().equals( third ) )
                            .getData()
                            .get( 0 )
                            .getData() );

        // Exceeds 50.0 with occurrences > 0.75
        MatrixOfDoubles expectedFourth = MatrixOfDoubles.of( new double[][] { { 37.0, 14.0 }, { 5.0, 109.0 } } );
        OneOrTwoThresholds fourth = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 50.0 ),
                                                                         Operator.GREATER,
                                                                         ThresholdDataType.LEFT ),
                                                           Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.75 ),
                                                                                             Operator.GREATER,
                                                                                             ThresholdDataType.LEFT ) );

        assertEquals( expectedFourth,
                      Slicer.filter( results, meta -> meta.getThresholds().equals( fourth ) )
                            .getData()
                            .get( 0 )
                            .getData() );

        // Exceeds 50.0 with occurrences > 0.9
        MatrixOfDoubles expectedFifth = MatrixOfDoubles.of( new double[][] { { 37.0, 11.0 }, { 5.0, 112.0 } } );
        OneOrTwoThresholds fifth = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.9 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        assertEquals( expectedFifth,
                      Slicer.filter( results, meta -> meta.getThresholds().equals( fifth ) )
                            .getData()
                            .get( 0 )
                            .getData() );

        // Exceeds 50.0 with occurrences > 0.95
        MatrixOfDoubles expectedSixth = MatrixOfDoubles.of( new double[][] { { 36.0, 10.0 }, { 6.0, 113.0 } } );
        OneOrTwoThresholds sixth = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 50.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ),
                                                          Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.95 ),
                                                                                            Operator.GREATER,
                                                                                            ThresholdDataType.LEFT ) );

        assertEquals( expectedSixth,
                      Slicer.filter( results, meta -> meta.getThresholds().equals( sixth ) )
                            .getData()
                            .get( 0 )
                            .getData() );

    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeEnsemblePairs} and application of
     * {@link MetricProcessorByTimeEnsemblePairs#apply(EnsemblePairs)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsFour()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithValueThresholdsAndNoData()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<EnsemblePairs, MetricOutputForProject> processor =
                MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config, MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsFour() );

        //Obtain the results
        ListOfMetricOutput<DoubleScoreOutput> results = processor.getCachedMetricOutput().getDoubleScoreOutput();

        //Validate the score outputs
        for ( DoubleScoreOutput nextMetric : results )
        {
            if ( nextMetric.getMetadata().getMetricID() != MetricConstants.SAMPLE_SIZE )
            {
                assertTrue( nextMetric.getData().isNaN() );
            }
        }
    }


}

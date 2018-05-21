package wres.engine.statistics.metric.processing;

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
import java.util.Map.Entry;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
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
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.thresholds.OneOrTwoThresholds;
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
     * Instance of a data factory.
     */

    private DataFactory dataFac;

    @Before
    public void setupBeforeEachTest()
    {
        dataFac = DefaultDataFactory.getInstance();
    }

    /**
     * Tests the {@link MetricProcessorByTimeEnsemblePairs#getFilterForEnsemblePairs(wres.datamodel.thresholds.Threshold)}.
     */

    @Test
    public void testGetFilterForEnsemblePairs()
    {
        OneOrTwoDoubles doubles = dataFac.ofOneOrTwoDoubles( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
                                                                                                          condition,
                                                                                                          ThresholdDataType.RIGHT_MEAN ) ) );
        // Check that average works        
        PairOfDoubleAndVectorOfDoubles pair = dataFac.pairOf( 1.0, new double[] { 1.5, 2.0 } );

        assertTrue( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( dataFac.ofThreshold( doubles,
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
        MetricProcessorByTime<EnsemblePairs> processor = MetricFactory.getInstance( dataFac )
                                                                      .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                                                             null );
        MetricOutputForProjectByTimeAndThreshold results =
                processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> bias = results.getDoubleScoreOutput()
                                                                           .get( MetricConstants.BIAS_FRACTION );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> cod =
                results.getDoubleScoreOutput()
                       .get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rho = results.getDoubleScoreOutput()
                                                                          .get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> mae = results.getDoubleScoreOutput()
                                                                          .get( MetricConstants.MEAN_ABSOLUTE_ERROR );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> me =
                results.getDoubleScoreOutput().get( MetricConstants.MEAN_ERROR );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rmse = results.getDoubleScoreOutput()
                                                                           .get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> crps =
                results.getDoubleScoreOutput()
                       .get( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );

        //Test contents
        assertTrue( "Unexpected difference in " + MetricConstants.BIAS_FRACTION,
                    bias.getValue( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                    cod.getValue( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                    rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                    mae.getValue( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ERROR,
                    me.getValue( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                    rmse.getValue( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                    Double.compare( crps.getValue( 0 ).getData(), 9.076475676968208 ) == 0 );
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
        //Obtain the results
        MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> results = processor.getCachedMetricOutput()
                                                                                     .getDoubleScoreOutput();

        //Validate bias
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 0 ),
                    bias.getValue( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 1 ),
                    bias.getValue( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 2 ),
                    bias.getValue( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 3 ),
                    bias.getValue( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 4 ),
                    bias.getValue( 4 ).getData().equals( -0.0505708024162773 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 5 ),
                    bias.getValue( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> cod =
                results.get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 0 ),
                    cod.getValue( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 1 ),
                    cod.getValue( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 2 ),
                    cod.getValue( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 3 ),
                    cod.getValue( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 4 ),
                    cod.getValue( 4 ).getData().equals( 0.7542039364210298 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 5 ),
                    cod.getValue( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rho =
                results.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 0 ),
                    rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 1 ),
                    rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 2 ),
                    rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 3 ),
                    rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 4 ),
                    rho.getValue( 4 ).getData().equals( 0.868449155921652 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 5 ),
                    rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 0 ),
                    mae.getValue( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 1 ),
                    mae.getValue( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 2 ),
                    mae.getValue( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 3 ),
                    mae.getValue( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 4 ),
                    mae.getValue( 4 ).getData().equals( 20.625668563442147 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 5 ),
                    mae.getValue( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> me = results.get( MetricConstants.MEAN_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 0 ),
                    me.getValue( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 1 ),
                    me.getValue( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 2 ),
                    me.getValue( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 3 ),
                    me.getValue( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 4 ),
                    me.getValue( 4 ).getData().equals( -3.4840043925326936 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 5 ),
                    me.getValue( 5 ).getData().equals( -4.218543908073952 ) );
        //Validate rmse
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rmse =
                results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 0 ),
                    rmse.getValue( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 1 ),
                    rmse.getValue( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 2 ),
                    rmse.getValue( 2 ).getData().equals( 52.55361580348336 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 3 ),
                    rmse.getValue( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 4 ),
                    rmse.getValue( 4 ).getData().equals( 58.12352988180837 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 5 ),
                    rmse.getValue( 5 ).getData().equals( 61.12163959516186 ) );
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

        MetricProcessorByTime<EnsemblePairs> processor = MetricFactory.getInstance( dataFac )
                                                                      .ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
                                                                                                             Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );

        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        // Obtain the results
        MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> actual = processor.getCachedMetricOutput()
                                                                                    .getDoubleScoreOutput();

        // Check for equality
        BiPredicate<Double, Double> testEqual = FunctionFactory.doubleEquals();

        assertTrue( testEqual.test( actual.get( MetricConstants.THREAT_SCORE ).getValue( 0 ).getData(),
                                    0.9160756501182034 ) );
        assertTrue( testEqual.test( actual.get( MetricConstants.PEIRCE_SKILL_SCORE ).getValue( 0 ).getData(),
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Obtain the results
        MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> results = processor.getCachedMetricOutput()
                                                                                     .getDoubleScoreOutput();

        //Validate a selection of the outputs only

        //Validate bias
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 0 ),
                    bias.getValue( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 1 ),
                    bias.getValue( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 2 ),
                    bias.getValue( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 3 ),
                    bias.getValue( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 4 ),
                    bias.getValue( 4 ).getData().equals( -0.05090288343061958 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 5 ),
                    bias.getValue( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> cod =
                results.get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 0 ),
                    cod.getValue( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 1 ),
                    cod.getValue( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 2 ),
                    cod.getValue( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 3 ),
                    cod.getValue( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 4 ),
                    cod.getValue( 4 ).getData().equals( 0.7540690263086123 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 5 ),
                    cod.getValue( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rho =
                results.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 0 ),
                    rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 1 ),
                    rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 2 ),
                    rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 3 ),
                    rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 4 ),
                    rho.getValue( 4 ).getData().equals( 0.8683714794421868 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 5 ),
                    rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 0 ),
                    mae.getValue( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 1 ),
                    mae.getValue( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 2 ),
                    mae.getValue( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 3 ),
                    mae.getValue( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 4 ),
                    mae.getValue( 4 ).getData().equals( 20.653785159500924 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 5 ),
                    mae.getValue( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> me = results.get( MetricConstants.MEAN_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 0 ),
                    me.getValue( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 1 ),
                    me.getValue( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 2 ),
                    me.getValue( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 3 ),
                    me.getValue( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 4 ),
                    me.getValue( 4 ).getData().equals( -3.5134287820490364 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 5 ),
                    me.getValue( 5 ).getData().equals( -4.218543908073952 ) );
        //Validate rmse
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rmse =
                results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 0 ),
                    rmse.getValue( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 1 ),
                    rmse.getValue( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 2 ),
                    rmse.getValue( 2 ).getData().equals( 52.55361580348336 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 3 ),
                    rmse.getValue( 3 ).getData().equals( 54.82426155439095 ) );

        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 4 ),
                    rmse.getValue( 4 ).getData().equals( 58.19124412599005 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 5 ),
                    rmse.getValue( 5 ).getData().equals( 61.12163959516186 ) );
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( new ProjectConfig( null,
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( new ProjectConfig( null,
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


        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
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

        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
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

        MetricFactory.getInstance( dataFac )
                     .ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
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

        MetricFactory.getInstance( dataFac )
                     .ofMetricProcessorByTimeEnsemblePairs( mockedConfig,
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( config,
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                    MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsOneWithMissings() );

        //Obtain the results
        MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> results = processor.getCachedMetricOutput()
                                                                                     .getDoubleScoreOutput();
        //Validate bias
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 0 ),
                    bias.getValue( 0 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 1 ),
                    bias.getValue( 1 ).getData().equals( -0.032093836077598345 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 2 ),
                    bias.getValue( 2 ).getData().equals( -0.0365931379807274 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 3 ),
                    bias.getValue( 3 ).getData().equals( -0.039706682985140816 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 4 ),
                    bias.getValue( 4 ).getData().equals( -0.0505708024162773 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                    + " at "
                    + bias.getKey( 5 ),
                    bias.getValue( 5 ).getData().equals( -0.056658160809530816 ) );
        //Validate CoD
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> cod =
                results.get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 0 ),
                    cod.getValue( 0 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 1 ),
                    cod.getValue( 1 ).getData().equals( 0.7873367083297588 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 2 ),
                    cod.getValue( 2 ).getData().equals( 0.7653639626077698 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 3 ),
                    cod.getValue( 3 ).getData().equals( 0.76063213080129 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 4 ),
                    cod.getValue( 4 ).getData().equals( 0.7542039364210298 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.COEFFICIENT_OF_DETERMINATION
                    + " at "
                    + cod.getKey( 5 ),
                    cod.getValue( 5 ).getData().equals( 0.7492338765733539 ) );
        //Validate rho
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rho =
                results.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 0 ),
                    rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 1 ),
                    rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 2 ),
                    rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 3 ),
                    rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 4 ),
                    rho.getValue( 4 ).getData().equals( 0.868449155921652 ) );
        assertTrue( "Expected results differ from actual results for "
                    + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                    + " at "
                    + rho.getKey( 5 ),
                    rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
        //Validate mae
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 0 ),
                    mae.getValue( 0 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 1 ),
                    mae.getValue( 1 ).getData().equals( 11.009512537315405 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 2 ),
                    mae.getValue( 2 ).getData().equals( 17.675554578575642 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 3 ),
                    mae.getValue( 3 ).getData().equals( 18.997815872635968 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 4 ),
                    mae.getValue( 4 ).getData().equals( 20.625668563442147 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ABSOLUTE_ERROR
                    + " at "
                    + mae.getKey( 5 ),
                    mae.getValue( 5 ).getData().equals( 22.094227646773568 ) );
        //Validate me
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> me = results.get( MetricConstants.MEAN_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 0 ),
                    me.getValue( 0 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 1 ),
                    me.getValue( 1 ).getData().equals( -1.157869354367079 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 2 ),
                    me.getValue( 2 ).getData().equals( -2.1250409720950105 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 3 ),
                    me.getValue( 3 ).getData().equals( -2.4855770739425846 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 4 ),
                    me.getValue( 4 ).getData().equals( -3.4840043925326936 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                    + " at "
                    + me.getKey( 5 ),
                    me.getValue( 5 ).getData().equals( -4.218543908073952 ) );
        //Validate rmse
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rmse =
                results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 0 ),
                    rmse.getValue( 0 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 1 ),
                    rmse.getValue( 1 ).getData().equals( 41.01563032408479 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 2 ),
                    rmse.getValue( 2 ).getData().equals( 52.55361580348336 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 3 ),
                    rmse.getValue( 3 ).getData().equals( 54.82426155439095 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 4 ),
                    rmse.getValue( 4 ).getData().equals( 58.12352988180837 ) );
        assertTrue( "Expected results differ from actual results for " + MetricConstants.ROOT_MEAN_SQUARE_ERROR
                    + " at "
                    + rmse.getKey( 5 ),
                    rmse.getValue( 5 ).getData().equals( 61.12163959516186 ) );
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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( config, MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );
        //Obtain the results
        MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> results = processor.getCachedMetricOutput()
                                                                                .getMatrixOutput();


        // Expected result
        final TimeWindow expectedWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( 24 ) );
        // Exceeds 50.0 with occurrences > 0.05
        MatrixOfDoubles expectedFirst = dataFac.matrixOf( new double[][] { { 40.0, 32.0 }, { 2.0, 91.0 } } );
        Pair<TimeWindow, OneOrTwoThresholds> first =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 50.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ),
                                                dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.05 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) ) );

        assertTrue( "Unexpected results for the contingency table.",
                    expectedFirst.equals( results.get( MetricConstants.CONTINGENCY_TABLE )
                                                 .get( first )
                                                 .getData() ) );

        // Exceeds 50.0 with occurrences > 0.25
        MatrixOfDoubles expectedSecond = dataFac.matrixOf( new double[][] { { 39.0, 17.0 }, { 3.0, 106.0 } } );
        Pair<TimeWindow, OneOrTwoThresholds> second =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 50.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ),
                                                dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.25 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) ) );

        assertTrue( "Unexpected results for the contingency table.",
                    expectedSecond.equals( results.get( MetricConstants.CONTINGENCY_TABLE )
                                                  .get( second )
                                                  .getData() ) );

        // Exceeds 50.0 with occurrences > 0.5
        MatrixOfDoubles expectedThird = dataFac.matrixOf( new double[][] { { 39.0, 15.0 }, { 3.0, 108.0 } } );
        Pair<TimeWindow, OneOrTwoThresholds> third =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 50.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ),
                                                dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.5 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) ) );

        assertTrue( "Unexpected results for the contingency table.",
                    expectedThird.equals( results.get( MetricConstants.CONTINGENCY_TABLE )
                                                 .get( third )
                                                 .getData() ) );

        // Exceeds 50.0 with occurrences > 0.75
        MatrixOfDoubles expectedFourth = dataFac.matrixOf( new double[][] { { 37.0, 14.0 }, { 5.0, 109.0 } } );
        Pair<TimeWindow, OneOrTwoThresholds> fourth =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 50.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ),
                                                dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.75 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) ) );

        assertTrue( "Unexpected results for the contingency table.",
                    expectedFourth.equals( results.get( MetricConstants.CONTINGENCY_TABLE )
                                                  .get( fourth )
                                                  .getData() ) );

        // Exceeds 50.0 with occurrences > 0.9
        MatrixOfDoubles expectedFifth = dataFac.matrixOf( new double[][] { { 37.0, 11.0 }, { 5.0, 112.0 } } );
        Pair<TimeWindow, OneOrTwoThresholds> fifth =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 50.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ),
                                                dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.9 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) ) );

        assertTrue( "Unexpected results for the contingency table.",
                    expectedFifth.equals( results.get( MetricConstants.CONTINGENCY_TABLE )
                                                 .get( fifth )
                                                 .getData() ) );

        // Exceeds 50.0 with occurrences > 0.95
        MatrixOfDoubles expectedSixth = dataFac.matrixOf( new double[][] { { 36.0, 10.0 }, { 6.0, 113.0 } } );
        Pair<TimeWindow, OneOrTwoThresholds> sixth =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 50.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ),
                                                dataFac.ofProbabilityThreshold( dataFac.ofOneOrTwoDoubles( 0.95 ),
                                                                                Operator.GREATER,
                                                                                ThresholdDataType.LEFT ) ) );

        assertTrue( "Unexpected results for the contingency table.",
                    expectedSixth.equals( results.get( MetricConstants.CONTINGENCY_TABLE )
                                                 .get( sixth )
                                                 .getData() ) );

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
        MetricProcessor<EnsemblePairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeEnsemblePairs( config, MetricOutputGroup.set() );
        processor.apply( MetricTestDataFactory.getEnsemblePairsFour() );

        //Obtain the results
        MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> results = processor.getCachedMetricOutput()
                                                                                     .getDoubleScoreOutput();

        //Validate the score outputs
        for ( MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> nextMetric : results.values() )
        {
            if ( nextMetric.getMetadata().getMetricID() != MetricConstants.SAMPLE_SIZE )
            {
                for ( Entry<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> nextOutput : nextMetric.entrySet() )
                {
                    assertTrue( "Expected results differ from actual results for "
                                + nextMetric.getMetadata().getMetricID()
                                + " at "
                                + nextOutput.getKey(),
                                nextOutput.getValue().getData().isNaN() );
                }
            }
        }
    }


}

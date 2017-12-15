package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;
import wres.datamodel.outputs.VectorOutput;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorEnsemblePairsByTime}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorEnsemblePairsByTimeTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByTime} and application of
     * {@link MetricProcessorEnsemblePairsByTime#apply(wres.datamodel.inputs.MetricInput)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsTest/test1ApplyNoThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     */

    @Test
    public void test1ApplyNoThresholds()
    {
        String configPath = "testinput/metricProcessorEnsemblePairsTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessorEnsemblePairsByTime processor =
                    (MetricProcessorEnsemblePairsByTime) MetricFactory.getInstance( dataFactory )
                                                                          .getMetricProcessorByTime( config );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            MetricOutputForProjectByTimeAndThreshold results = processor.apply( pairs );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> bias = results.getScalarOutput()
                                                                       .get( MetricConstants.BIAS_FRACTION );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> cod =
                    results.getScalarOutput()
                           .get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rho = results.getScalarOutput()
                                                                      .get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> mae = results.getScalarOutput()
                                                                      .get( MetricConstants.MEAN_ABSOLUTE_ERROR );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> me =
                    results.getScalarOutput().get( MetricConstants.MEAN_ERROR );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rmse = results.getScalarOutput()
                                                                       .get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
            MetricOutputMapByTimeAndThreshold<VectorOutput> crps =
                    results.getVectorOutput()
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
                        Double.compare( crps.getValue( 0 ).getData().getDoubles()[0], 9.076475676968208 ) == 0 );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByTime} and application of
     * {@link MetricProcessorEnsemblePairsByTime#apply(wres.datamodel.inputs.MetricInput)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsTest/test2ApplyWithValueThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     */

    @Test
    public void test2ApplyWithValueThresholds()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorEnsemblePairsTest/test2ApplyWithValueThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.values() );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            processor.apply( pairs );
            //Obtain the results
            MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> results = processor.getStoredMetricOutput()
                                                                                 .getScalarOutput();
            //Validate bias
            MetricOutputMapByTimeAndThreshold<ScalarOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> cod =
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rho = results.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 0 ),
                        rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 1 ),
                        rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 2 ),
                        rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 3 ),
                        rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 4 ),
                        rho.getValue( 4 ).getData().equals( 0.868449155921652 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 5 ),
                        rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
            //Validate mae
            MetricOutputMapByTimeAndThreshold<ScalarOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> me = results.get( MetricConstants.MEAN_ERROR );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rmse = results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
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
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByTime} and application of
     * {@link MetricProcessorEnsemblePairsByTime#apply(wres.datamodel.inputs.MetricInput)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsTest/test3ApplyWithProbabilityThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     */

    @Test
    public void test3ApplyWithProbabilityThresholds()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorEnsemblePairsTest/test3ApplyWithProbabilityThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            processor.apply( pairs );
            //Obtain the results
            MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> results = processor.getStoredMetricOutput()
                                                                                 .getScalarOutput();

            //Validate a selection of the outputs only

            //Validate bias
            MetricOutputMapByTimeAndThreshold<ScalarOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> cod =
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rho = results.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 0 ),
                        rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 1 ),
                        rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 2 ),
                        rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 3 ),
                        rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );       
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 4 ),
                        rho.getValue( 4 ).getData().equals( 0.8683714794421868 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 5 ),
                        rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
            //Validate mae
            MetricOutputMapByTimeAndThreshold<ScalarOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> me = results.get( MetricConstants.MEAN_ERROR );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rmse = results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
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
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests for exceptions associated with a {@link MetricProcessorEnsemblePairsByTime}.
     */

    @Test
    public void test4Exceptions()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String testOne = "testinput/metricProcessorEnsemblePairsTest/test4ExceptionsOne.xml";
        try
        {
            ProjectConfig config =
                    ProjectConfigPlus.from( Paths.get( testOne ) )
                                     .getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( null );
            fail( "Expected a checked exception on processing the project configuration '" + testOne + "'." );
        }
        catch ( Exception e )
        {
        }
        //Check for fail on insufficient data with ensemble metrics
        String testTwo = "testinput/metricProcessorEnsemblePairsTest/test4ExceptionsTwo.xml";
        try
        {
            ProjectConfig config =
                    ProjectConfigPlus.from( Paths.get( testTwo ) )
                                     .getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );
            fail( "Expected a checked exception on processing the project configuration '" + testTwo
                  + "' with insufficient data." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testTwo
                  + "' with insufficient data." );
        }
        //Check for fail on insufficient data with discrete probability metrics
        String testThree = "testinput/metricProcessorEnsemblePairsTest/test4ExceptionsThree.xml";
        try
        {
            ProjectConfig config =
                    ProjectConfigPlus.from( Paths.get( testThree ) )
                                     .getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );
            fail( "Expected a checked exception on processing the project configuration '" + testThree
                  + "' with insufficient data." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testThree
                  + "' with insufficient data." );
        }
        //Check for fail on configuration of a dichotomous metric
        String testFour = "testinput/metricProcessorEnsemblePairsTest/test4ExceptionsFour.xml";
        try
        {
            ProjectConfig config =
                    ProjectConfigPlus.from( Paths.get( testFour ) )
                                     .getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );
            fail( "Expected a checked exception on processing the project configuration '" + testFour
                  + "' with a dichotomous metric." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testFour
                  + "' with a dichotomous metric." );
        }
        //Check for fail on configuration of a multicategory metric
        String testFive = "testinput/metricProcessorEnsemblePairsTest/test4ExceptionsFive.xml";
        try
        {
            ProjectConfig config =
                    ProjectConfigPlus.from( Paths.get( testFive ) )
                                     .getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );
            fail( "Expected a checked exception on processing the project configuration '" + testFive
                  + "' with a multicategory metric." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testFive
                  + "' with a multicategory metric." );
        }        
        //Check for fail on configuration of a skill metric that requires a baseline, with no baseline configured
        String testSix = "testinput/metricProcessorEnsemblePairsTest/test4ExceptionsSix.xml";
        try
        {
            ProjectConfig config =
                    ProjectConfigPlus.from( Paths.get( testSix ) )
                                     .getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );
            fail( "Expected a checked exception on processing the project configuration '" + testSix
                  + "' with a skill metric that requires a baseline, in the absence of a baseline." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testSix
                  + "' with a skill metric that requires a baseline, in the absence of a baseline." );
        }          
    }
    
    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByTime} for all valid metrics associated
     * with ensemble inputs.
     */

    @Test
    public void test5AllValid()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorEnsemblePairsTest/test5AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.values() );
            //Check for the expected number of metrics
            //One fewer than total, as sample size appears in both ensemble and single-valued
            assertTrue( processor.metrics.size() == MetricInputGroup.ENSEMBLE.getMetrics().size()
                                                    + MetricInputGroup.DISCRETE_PROBABILITY.getMetrics().size()
                                                    + MetricInputGroup.SINGLE_VALUED.getMetrics().size() -1 );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }  
    
    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByTime} and application of
     * {@link MetricProcessorEnsemblePairsByTime#apply(wres.datamodel.inputs.MetricInput)} to configuration 
     * obtained from testinput/metricProcessorEnsemblePairsTest/test2ApplyWithValueThresholds.xml and pairs obtained 
     * from {@link MetricTestDataFactory#getEnsemblePairsOneWithMissings()}.
     */

    @Test
    public void test6ApplyWithValueThresholdsAndMissings()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorEnsemblePairsTest/test2ApplyWithValueThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.values() );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOneWithMissings();
            processor.apply( pairs );
            //Obtain the results
            MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> results = processor.getStoredMetricOutput()
                                                                                 .getScalarOutput();
            //Validate bias
            MetricOutputMapByTimeAndThreshold<ScalarOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> cod =
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rho = results.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 0 ),
                        rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 1 ),
                        rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 2 ),
                        rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 3 ),
                        rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 4 ),
                        rho.getValue( 4 ).getData().equals( 0.868449155921652 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                        + " at "
                        + rho.getKey( 5 ),
                        rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
            //Validate mae
            MetricOutputMapByTimeAndThreshold<ScalarOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> me = results.get( MetricConstants.MEAN_ERROR );
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
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rmse = results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
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
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }    
    

}

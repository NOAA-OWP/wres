package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;
import java.util.TreeSet;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.ScalarOutput;
import wres.datamodel.Threshold;
import wres.datamodel.VectorOutput;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorEnsemblePairsByLeadTime}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorEnsemblePairsTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByLeadTime} and application of
     * {@link MetricProcessorEnsemblePairsByLeadTime#apply(wres.datamodel.EnsemblePairs)} to configuration obtained from
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
            MetricProcessorEnsemblePairsByLeadTime processor =
                    (MetricProcessorEnsemblePairsByLeadTime) MetricFactory.getInstance( dataFactory )
                                                                          .getMetricProcessorByLeadTime( config );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            MetricOutputForProjectByLeadThreshold results = processor.apply( pairs );
            MetricOutputMapByLeadThreshold<ScalarOutput> bias = results.getScalarOutput()
                                                                       .get( MetricConstants.BIAS_FRACTION );
            MetricOutputMapByLeadThreshold<ScalarOutput> cod =
                    results.getScalarOutput()
                           .get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
            MetricOutputMapByLeadThreshold<ScalarOutput> rho = results.getScalarOutput()
                                                                      .get( MetricConstants.CORRELATION_PEARSONS );
            MetricOutputMapByLeadThreshold<ScalarOutput> mae = results.getScalarOutput()
                                                                      .get( MetricConstants.MEAN_ABSOLUTE_ERROR );
            MetricOutputMapByLeadThreshold<ScalarOutput> me =
                    results.getScalarOutput().get( MetricConstants.MEAN_ERROR );
            MetricOutputMapByLeadThreshold<ScalarOutput> rmse = results.getScalarOutput()
                                                                       .get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
            MetricOutputMapByLeadThreshold<VectorOutput> crps =
                    results.getVectorOutput()
                           .get( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE, MetricConstants.NONE );

            //Test contents
            assertTrue( "Unexpected difference in " + MetricConstants.BIAS_FRACTION,
                        bias.getValue( 0 ).getData().equals( 0.032093836077598345 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                        cod.getValue( 0 ).getData().equals( 0.7873367083297588 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.CORRELATION_PEARSONS,
                        rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                        mae.getValue( 0 ).getData().equals( 11.009512537315405 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ERROR,
                        me.getValue( 0 ).getData().equals( 1.157869354367079 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                        rmse.getValue( 0 ).getData().equals( 41.01563032408479 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                        Double.compare( crps.getValue( 0 ).getData().getDoubles()[0], 9.075772555092389 ) == 0 );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByLeadTime} and application of
     * {@link MetricProcessorEnsemblePairsByLeadTime#apply(wres.datamodel.EnsemblePairs)} to configuration obtained from
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
            MetricProcessor<MetricOutputForProjectByLeadThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config,
                                                                MetricOutputGroup.SCALAR );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            processor.apply( pairs );
            //Obtain the results
            MetricOutputMultiMapByLeadThreshold<ScalarOutput> results = processor.getStoredMetricOutput()
                                                                                 .getScalarOutput();
            //Validate bias
            MetricOutputMapByLeadThreshold<ScalarOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 0 ),
                        bias.getValue( 0 ).getData().equals( 0.032093836077598345 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 1 ),
                        bias.getValue( 1 ).getData().equals( 0.032093836077598345 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 2 ),
                        bias.getValue( 2 ).getData().equals( 0.0365931379807274 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 3 ),
                        bias.getValue( 3 ).getData().equals( 0.039706682985140816 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 4 ),
                        bias.getValue( 4 ).getData().equals( 0.0505708024162773 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 5 ),
                        bias.getValue( 5 ).getData().equals( 0.056658160809530816 ) );
            //Validate CoD
            MetricOutputMapByLeadThreshold<ScalarOutput> cod =
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
            MetricOutputMapByLeadThreshold<ScalarOutput> rho = results.get( MetricConstants.CORRELATION_PEARSONS );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 0 ),
                        rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 1 ),
                        rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 2 ),
                        rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 3 ),
                        rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 4 ),
                        rho.getValue( 4 ).getData().equals( 0.868449155921652 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 5 ),
                        rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
            //Validate mae
            MetricOutputMapByLeadThreshold<ScalarOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
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
            MetricOutputMapByLeadThreshold<ScalarOutput> me = results.get( MetricConstants.MEAN_ERROR );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 0 ),
                        me.getValue( 0 ).getData().equals( 1.157869354367079 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 1 ),
                        me.getValue( 1 ).getData().equals( 1.157869354367079 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 2 ),
                        me.getValue( 2 ).getData().equals( 2.1250409720950105 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 3 ),
                        me.getValue( 3 ).getData().equals( 2.4855770739425846 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 4 ),
                        me.getValue( 4 ).getData().equals( 3.4840043925326936 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 5 ),
                        me.getValue( 5 ).getData().equals( 4.218543908073952 ) );
            //Validate rmse
            MetricOutputMapByLeadThreshold<ScalarOutput> rmse = results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
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
            e.printStackTrace();
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairsByLeadTime} and application of
     * {@link MetricProcessorEnsemblePairsByLeadTime#apply(wres.datamodel.EnsemblePairs)} to configuration obtained from
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
            MetricProcessor<MetricOutputForProjectByLeadThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config,
                                                                MetricOutputGroup.SCALAR );
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            processor.apply( pairs );
            //Obtain the results
            MetricOutputMultiMapByLeadThreshold<ScalarOutput> results = processor.getStoredMetricOutput()
                                                                                 .getScalarOutput();

            TreeSet<Threshold> s = new TreeSet<>();

            results.get( MetricConstants.BIAS_FRACTION ).keySetByThreshold().forEach( res -> s.add( res ) );
            ;

            //Validate bias
            MetricOutputMapByLeadThreshold<ScalarOutput> bias = results.get( MetricConstants.BIAS_FRACTION );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 0 ),
                        bias.getValue( 0 ).getData().equals( 0.032093836077598345 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 1 ),
                        bias.getValue( 1 ).getData().equals( 0.032093836077598345 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 2 ),
                        bias.getValue( 2 ).getData().equals( 0.0365931379807274 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 3 ),
                        bias.getValue( 3 ).getData().equals( 0.039706682985140816 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 4 ),
                        bias.getValue( 4 ).getData().equals( 0.0505708024162773 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.BIAS_FRACTION
                        + " at "
                        + bias.getKey( 5 ),
                        bias.getValue( 5 ).getData().equals( 0.056658160809530816 ) );
            //Validate CoD
            MetricOutputMapByLeadThreshold<ScalarOutput> cod =
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
            MetricOutputMapByLeadThreshold<ScalarOutput> rho = results.get( MetricConstants.CORRELATION_PEARSONS );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 0 ),
                        rho.getValue( 0 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 1 ),
                        rho.getValue( 1 ).getData().equals( 0.8873199582618204 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 2 ),
                        rho.getValue( 2 ).getData().equals( 0.8748508230594344 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 3 ),
                        rho.getValue( 3 ).getData().equals( 0.8721422652304439 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 4 ),
                        rho.getValue( 4 ).getData().equals( 0.868449155921652 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.CORRELATION_PEARSONS
                        + " at "
                        + rho.getKey( 5 ),
                        rho.getValue( 5 ).getData().equals( 0.8655829692024641 ) );
            //Validate mae
            MetricOutputMapByLeadThreshold<ScalarOutput> mae = results.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
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
            MetricOutputMapByLeadThreshold<ScalarOutput> me = results.get( MetricConstants.MEAN_ERROR );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 0 ),
                        me.getValue( 0 ).getData().equals( 1.157869354367079 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 1 ),
                        me.getValue( 1 ).getData().equals( 1.157869354367079 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 2 ),
                        me.getValue( 2 ).getData().equals( 2.1250409720950105 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 3 ),
                        me.getValue( 3 ).getData().equals( 2.4855770739425846 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 4 ),
                        me.getValue( 4 ).getData().equals( 3.4840043925326936 ) );
            assertTrue( "Expected results differ from actual results for " + MetricConstants.MEAN_ERROR
                        + " at "
                        + me.getKey( 5 ),
                        me.getValue( 5 ).getData().equals( 4.218543908073952 ) );
            //Validate rmse
            MetricOutputMapByLeadThreshold<ScalarOutput> rmse = results.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
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
            e.printStackTrace();
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

}

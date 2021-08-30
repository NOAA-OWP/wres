package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.Test;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MetricProcessorByTimeEnsemblePairs}.
 * 
 * @author James Brown
 */
public final class MetricProcessorByTimeEnsemblePairsTest
{
    /**
     * Test source.
     */

    private static final String TEST_SOURCE =
            "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

    @Test
    public void testGetFilterForEnsemblePairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                        condition,
                                                                                                        ThresholdDataType.RIGHT_MEAN ) ) );
        // Check that average works        
        Pair<Double, Ensemble> pair = Pair.of( 1.0, Ensemble.of( 1.5, 2.0 ) );

        assertTrue( MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                     condition,
                                                                                                     ThresholdDataType.RIGHT_MEAN ) )
                                                      .test( pair ) );
    }

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithoutThresholds.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );
        StatisticsForProject results =
                processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );
        DoubleScoreStatisticOuter bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION ).get( 0 );
        DoubleScoreStatisticOuter cod =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.COEFFICIENT_OF_DETERMINATION )
                      .get( 0 );
        DoubleScoreStatisticOuter rho =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                      .get( 0 );
        DoubleScoreStatisticOuter mae =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ABSOLUTE_ERROR )
                      .get( 0 );
        DoubleScoreStatisticOuter me =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR ).get( 0 );
        DoubleScoreStatisticOuter rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                      .get( 0 );
        DoubleScoreStatisticOuter crps =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE )
                      .get( 0 );

        assertEquals( bias.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      -0.032093836077598345,
                      Precision.EPSILON );
        assertEquals( cod.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      0.7873367083297588,
                      Precision.EPSILON );
        assertEquals( rho.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      0.8873199582618204,
                      Precision.EPSILON );
        assertEquals( mae.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      11.009512537315405,
                      Precision.EPSILON );
        assertEquals( me.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      -1.157869354367079,
                      Precision.EPSILON );
        assertEquals( rmse.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      41.01563032408479,
                      Precision.EPSILON );
        assertEquals( crps.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      9.076475676968208,
                      Precision.EPSILON );
    }

    @Test
    public void testApplyWithValueThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );
        StatisticsForProject statistics = processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.BIAS_FRACTION );
        assertEquals( -0.032093836077598345,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.032093836077598345,
                      bias.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0365931379807274,
                      bias.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.039706682985140816,
                      bias.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0505708024162773,
                      bias.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.056658160809530816,
                      bias.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate CoD
        List<DoubleScoreStatisticOuter> cod = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );

        assertEquals( 0.7873367083297588,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7653639626077698,
                      cod.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.76063213080129,
                      cod.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7542039364210298,
                      cod.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7492338765733539,
                      cod.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate rho
        List<DoubleScoreStatisticOuter> rho = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        assertEquals( 0.8873199582618204,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8748508230594344,
                      rho.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8721422652304439,
                      rho.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.868449155921652,
                      rho.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8655829692024641,
                      rho.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR );

        assertEquals( 11.009512537315405,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 17.675554578575642,
                      mae.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 18.997815872635968,
                      mae.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 20.625668563442147,
                      mae.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 22.094227646773568,
                      mae.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                            MetricConstants.MEAN_ERROR );

        assertEquals( -1.157869354367079,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.1250409720950105,
                      me.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.4855770739425846,
                      me.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -3.4840043925326936,
                      me.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -4.2185439080739515,
                      me.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        assertEquals( 41.01563032408479,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 52.55361580348335,
                      rmse.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 54.82426155439095,
                      rmse.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 58.12352988180837,
                      rmse.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 61.12163959516186,
                      rmse.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testApplyWithValueThresholdsAndCategoricalMeasures()
            throws MetricParameterException, IOException, InterruptedException
    {
        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.THREAT_SCORE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEIRCE_SKILL_SCORE ) );

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
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig );

        StatisticsForProject statistics = processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        // Obtain the results
        List<DoubleScoreStatisticOuter> actual = statistics.getDoubleScoreStatistics();

        assertEquals( Slicer.filter( actual, MetricConstants.THREAT_SCORE )
                            .get( 0 )
                            .getComponent( MetricConstants.MAIN )
                            .getData()
                            .getValue(),
                      0.9160756501182034,
                      Precision.EPSILON );

        assertEquals( Slicer.filter( actual, MetricConstants.PEIRCE_SKILL_SCORE )
                            .get( 0 )
                            .getComponent( MetricConstants.MAIN )
                            .getData()
                            .getValue(),
                      -0.0012886597938144284,
                      Precision.EPSILON );
    }

    @Test
    public void testApplyWithProbabilityThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithProbabilityThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );
        StatisticsForProject statistics = processor.apply( MetricTestDataFactory.getEnsemblePairsOne() );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        //Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION );
        assertEquals( -0.032093836077598345,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.032093836077598345,
                      bias.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0365931379807274,
                      bias.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.039706682985140816,
                      bias.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.05090288343061958,
                      bias.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.056658160809530816,
                      bias.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate CoD
        List<DoubleScoreStatisticOuter> cod =
                Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertEquals( 0.7873367083297588,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7653639626077698,
                      cod.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.76063213080129,
                      cod.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7540690263086123,
                      cod.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7492338765733539,
                      cod.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate rho
        List<DoubleScoreStatisticOuter> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        assertEquals( 0.8873199582618204,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8748508230594344,
                      rho.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8721422652304439,
                      rho.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8683714794421868,
                      rho.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8655829692024641,
                      rho.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR );

        assertEquals( 11.009512537315405,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 17.675554578575642,
                      mae.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 18.997815872635968,
                      mae.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 20.653785159500924,
                      mae.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 22.094227646773568,
                      mae.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( results, MetricConstants.MEAN_ERROR );

        assertEquals( -1.157869354367079,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.1250409720950105,
                      me.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.4855770739425846,
                      me.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -3.5134287820490364,
                      me.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -4.2185439080739515,
                      me.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertEquals( 41.01563032408479,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 52.55361580348335,
                      rmse.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 54.82426155439095,
                      rmse.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 58.191244125990046,
                      rmse.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 61.12163959516186,
                      rmse.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testExceptionOnNullInput() throws MetricParameterException
    {
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( new ProjectConfig( null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null ) );

        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> processor.apply( null ) );

        assertEquals( "Expected non-null input to the metric processor.", actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricParameterException, IOException
    {
        MetricsConfig metrics =
                new MetricsConfig( null,
                                   0,
                                   Arrays.asList( new MetricConfig( null, MetricConfigName.BRIER_SCORE ) ),
                                   null );

        MetricConfigException actual = assertThrows( MetricConfigException.class,
                                                     () -> MetricFactory.ofMetricProcessorForEnsemblePairs( new ProjectConfig( null,
                                                                                                                               null,
                                                                                                                               Arrays.asList( metrics ),
                                                                                                                               null,
                                                                                                                               null,
                                                                                                                               null ) ) );

        assertEquals( "Cannot configure 'BRIER SCORE' without thresholds to define the events: "
                      + "add one or more thresholds to the configuration.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricParameterException
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BRIER_SCORE ) );

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
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig );

        MetricCalculationException actual = assertThrows( MetricCalculationException.class,
                                                          () -> processor.apply( MetricTestDataFactory.getEnsemblePairsThree() ) );

        assertEquals( "Unable to determine quantile threshold from probability threshold: no climatological "
                      + "observations were available in the input.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionWhenBaselineIsMissing()
            throws MetricParameterException
    {
        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricConfigException actual = assertThrows( MetricConfigException.class,
                                                     () -> MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig ) );

        assertEquals( "Specify a non-null baseline from which to generate the 'CONTINUOUS RANKED "
                      + "PROBABILITY SKILL SCORE'.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionForDichotomousMetricWhenClassifierThresholdsAreMissing()
            throws MetricParameterException
    {
        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.THREAT_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricConfigException actual = assertThrows( MetricConfigException.class,
                                                     () -> MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig ) );

        assertEquals( "In order to configure dichotomous metrics for ensemble inputs, every metric group "
                      + "that contains dichotomous metrics must also contain thresholds for classifying the forecast "
                      + "probabilities into occurrences and non-occurrences.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionForMulticategoryMetricWhenClassifierThresholdsAreMissing()
            throws MetricParameterException
    {
        // Mock configuration
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.PEIRCE_SKILL_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "1.0",
                                              ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mockedConfig =
                new ProjectConfig( new Inputs( null, null, null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricConfigException actual = assertThrows( MetricConfigException.class,
                                                     () -> MetricFactory.ofMetricProcessorForEnsemblePairs( mockedConfig ) );

        assertEquals( "In order to configure multicategory metrics for ensemble inputs, every metric "
                      + "group that contains multicategory metrics must also contain thresholds for classifying the "
                      + "forecast probabilities into occurrences and non-occurrences.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyWithAllValidMetrics() throws IOException, MetricParameterException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testAllValid.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );

        //Check for the expected number of metrics
        //One fewer than total, as sample size appears in both ensemble and single-valued
        assertTrue( processor.metrics.getMetrics().size() == SampleDataGroup.ENSEMBLE.getMetrics().size()
                                                             + SampleDataGroup.DISCRETE_PROBABILITY.getMetrics().size()
                                                             + SampleDataGroup.SINGLE_VALUED.getMetrics().size()
                                                             - 1 );
    }

    @Test
    public void testApplyWithValueThresholdsAndMissings()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );

        StatisticsForProject statistics = processor.apply( MetricTestDataFactory.getEnsemblePairsOneWithMissings() );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        //Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION );
        assertEquals( -0.032093836077598345,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.032093836077598345,
                      bias.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0365931379807274,
                      bias.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.039706682985140816,
                      bias.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0505708024162773,
                      bias.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.056658160809530816,
                      bias.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate CoD
        List<DoubleScoreStatisticOuter> cod =
                Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertEquals( 0.7873367083297588,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7653639626077698,
                      cod.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.76063213080129,
                      cod.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7542039364210298,
                      cod.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7492338765733539,
                      cod.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate rho
        List<DoubleScoreStatisticOuter> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        assertEquals( 0.8873199582618204,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8748508230594344,
                      rho.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8721422652304439,
                      rho.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.868449155921652,
                      rho.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8655829692024641,
                      rho.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR );

        assertEquals( 11.009512537315405,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 17.675554578575642,
                      mae.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 18.997815872635968,
                      mae.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 20.625668563442147,
                      mae.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 22.094227646773568,
                      mae.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( results, MetricConstants.MEAN_ERROR );

        assertEquals( -1.157869354367079,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.1250409720950105,
                      me.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.4855770739425846,
                      me.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -3.4840043925326936,
                      me.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -4.2185439080739515,
                      me.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertEquals( 41.01563032408479,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.get( 1 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 52.55361580348335,
                      rmse.get( 2 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 54.82426155439095,
                      rmse.get( 3 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 58.12352988180837,
                      rmse.get( 4 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 61.12163959516186,
                      rmse.get( 5 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testContingencyTable()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorEnsemblePairsByTimeTest/testContingencyTable.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );
        StatisticsForProject statistics = processor.apply( MetricTestDataFactory.getEnsemblePairsTwo() );

        // Expected result
        final TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                   Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                   Duration.ofHours( 24 ) );

        FeatureKey featureKey = FeatureKey.of( "DRRC2" );
        FeatureTuple featureTuple = new FeatureTuple( featureKey, featureKey, featureKey );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( featureTuple,
                                                                    expectedWindow,
                                                                    null,
                                                                    null,
                                                                    false );

        PoolMetadata expectedSampleMeta = PoolMetadata.of( evaluation, pool );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results =
                Slicer.filter( statistics.getDoubleScoreStatistics(),
                               meta -> meta.getMetricName().equals( MetricConstants.CONTINGENCY_TABLE )
                                       && meta.getMetadata().getTimeWindow().equals( expectedWindow ) );

        // Exceeds 50.0 with occurrences > 0.05
        DoubleScoreStatistic firstTable =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 40 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 32 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 2 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 91 ) )
                                    .build();

        OneOrTwoThresholds first = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.05 ),
                                                                                                 Operator.GREATER,
                                                                                                 ThresholdDataType.LEFT ) );

        PoolMetadata expectedMetaFirst = PoolMetadata.of( expectedSampleMeta, first );

        DoubleScoreStatisticOuter expectedFirst =
                DoubleScoreStatisticOuter.of( firstTable, expectedMetaFirst );

        DoubleScoreStatisticOuter actualFirst =
                Slicer.filter( results, meta -> meta.getMetadata().getThresholds().equals( first ) )
                      .get( 0 );

        assertEquals( expectedFirst, actualFirst );

        // Exceeds 50.0 with occurrences > 0.25
        DoubleScoreStatistic secondTable =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 39 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 17 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 3 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 106 ) )
                                    .build();

        OneOrTwoThresholds second = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ),
                                                           ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.25 ),
                                                                                                  Operator.GREATER,
                                                                                                  ThresholdDataType.LEFT ) );

        PoolMetadata expectedMetaSecond = PoolMetadata.of( expectedSampleMeta, second );

        DoubleScoreStatisticOuter expectedSecond =
                DoubleScoreStatisticOuter.of( secondTable, expectedMetaSecond );

        DoubleScoreStatisticOuter actualSecond =
                Slicer.filter( results, meta -> meta.getMetadata().getThresholds().equals( second ) )
                      .get( 0 );

        assertEquals( expectedSecond, actualSecond );

        // Exceeds 50.0 with occurrences > 0.5
        DoubleScoreStatistic thirdTable =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 39 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 15 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 3 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 108 ) )
                                    .build();

        OneOrTwoThresholds third = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.5 ),
                                                                                                 Operator.GREATER,
                                                                                                 ThresholdDataType.LEFT ) );

        PoolMetadata expectedMetaThird = PoolMetadata.of( expectedSampleMeta, third );

        DoubleScoreStatisticOuter expectedThird =
                DoubleScoreStatisticOuter.of( thirdTable, expectedMetaThird );

        DoubleScoreStatisticOuter actualThird =
                Slicer.filter( results, meta -> meta.getMetadata().getThresholds().equals( third ) )
                      .get( 0 );

        assertEquals( expectedThird, actualThird );

        // Exceeds 50.0 with occurrences > 0.75
        DoubleScoreStatistic fourthTable =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 37 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 14 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 5 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 109 ) )
                                    .build();

        OneOrTwoThresholds fourth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                              Operator.GREATER,
                                                                              ThresholdDataType.LEFT ),
                                                           ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.75 ),
                                                                                                  Operator.GREATER,
                                                                                                  ThresholdDataType.LEFT ) );

        PoolMetadata expectedMetaFourth = PoolMetadata.of( expectedSampleMeta, fourth );

        DoubleScoreStatisticOuter expectedFourth =
                DoubleScoreStatisticOuter.of( fourthTable, expectedMetaFourth );
        DoubleScoreStatisticOuter actualFourth =
                Slicer.filter( results, meta -> meta.getMetadata().getThresholds().equals( fourth ) )
                      .get( 0 );

        assertEquals( expectedFourth, actualFourth );

        // Exceeds 50.0 with occurrences > 0.9
        DoubleScoreStatistic fifthTable =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 37 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 11 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 5 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 112 ) )
                                    .build();

        OneOrTwoThresholds fifth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.9 ),
                                                                                                 Operator.GREATER,
                                                                                                 ThresholdDataType.LEFT ) );

        PoolMetadata expectedMetaFifth = PoolMetadata.of( expectedSampleMeta, fifth );

        DoubleScoreStatisticOuter expectedFifth =
                DoubleScoreStatisticOuter.of( fifthTable, expectedMetaFifth );

        DoubleScoreStatisticOuter actualFifth =
                Slicer.filter( results, meta -> meta.getMetadata().getThresholds().equals( fifth ) )
                      .get( 0 );

        assertEquals( expectedFifth, actualFifth );

        // Exceeds 50.0 with occurrences > 0.95
        DoubleScoreStatistic sixthTable =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 36 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 10 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 6 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 113 ) )
                                    .build();

        OneOrTwoThresholds sixth = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT ),
                                                          ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.95 ),
                                                                                                 Operator.GREATER,
                                                                                                 ThresholdDataType.LEFT ) );

        PoolMetadata expectedMetaSixth = PoolMetadata.of( expectedSampleMeta, sixth );

        DoubleScoreStatisticOuter expectedSixth =
                DoubleScoreStatisticOuter.of( sixthTable, expectedMetaSixth );

        DoubleScoreStatisticOuter actualSixth =
                Slicer.filter( results, meta -> meta.getMetadata().getThresholds().equals( sixth ) )
                      .get( 0 );

        assertEquals( expectedSixth, actualSixth );

    }

    @Test
    public void testApplyWithValueThresholdsAndNoData()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( config );
        StatisticsForProject statistics = processor.apply( MetricTestDataFactory.getEnsemblePairsFour() );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        //Validate the score outputs
        for ( DoubleScoreStatisticOuter nextMetric : results )
        {
            if ( nextMetric.getMetricName() != MetricConstants.SAMPLE_SIZE )
            {
                nextMetric.forEach( next -> assertEquals( Double.NaN, next.getData().getValue(), 0.0 ) );
            }
        }
    }

    @Test
    public void testThatSampleSizeIsConstructedForEnsembleInput() throws MetricParameterException
    {
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.SAMPLE_SIZE ) );

        ProjectConfig mock =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mock );

        Set<MetricConstants> actualSingleValuedScores =
                Set.of( processor.getMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedSingleValuedScores = Set.of( MetricConstants.MEAN_ERROR );

        assertEquals( expectedSingleValuedScores, actualSingleValuedScores );

        Set<MetricConstants> actualEnsembleScores =
                Set.of( processor.getMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedEnsembleScores =
                Set.of( MetricConstants.SAMPLE_SIZE,
                        MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );

        assertEquals( expectedEnsembleScores, actualEnsembleScores );

    }

    @Test
    public void testThatSampleSizeIsConstructedForEnsembleInputWhenProbailityScoreExists()
            throws MetricParameterException
    {
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.BRIER_SCORE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.SAMPLE_SIZE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( wres.config.generated.ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              wres.config.generated.ThresholdOperator.GREATER_THAN ) );

        ProjectConfig mock =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
                MetricFactory.ofMetricProcessorForEnsemblePairs( mock );

        Set<MetricConstants> actualSingleValuedScores =
                Set.of( processor.getMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedSingleValuedScores = Set.of( MetricConstants.MEAN_ERROR );

        assertEquals( expectedSingleValuedScores, actualSingleValuedScores );

        Set<MetricConstants> actualEnsembleScores =
                Set.of( processor.getMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedEnsembleScores = Set.of( MetricConstants.SAMPLE_SIZE );

        assertEquals( expectedEnsembleScores, actualEnsembleScores );


        Set<MetricConstants> actualProbabilityScores =
                Set.of( processor.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE ) );

        Set<MetricConstants> expectedProbabilityScores = Set.of( MetricConstants.BRIER_SCORE );

        assertEquals( expectedProbabilityScores, actualProbabilityScores );

    }

}

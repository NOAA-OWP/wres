package wres.pipeline.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import com.google.protobuf.DoubleValue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.Test;


import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.thresholds.ThresholdException;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.metrics.MetricParameterException;
import wres.metrics.categorical.ContingencyTable;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link EnsembleStatisticsProcessor}.
 *
 * @author James Brown
 */
public final class EnsembleStatisticsProcessorTest
{

    @Test
    public void testGetFilterForEnsemblePairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        wres.config.yaml.components.ThresholdOperator condition = wres.config.yaml.components.ThresholdOperator.GREATER;
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.LEFT ) ) );
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.RIGHT ) ) );
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.LEFT_AND_RIGHT ) ) );
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.ANY_RIGHT ) ) );
        assertNotNull( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                                 condition,
                                                                                                 ThresholdOrientation.RIGHT_MEAN ) ) );
        // Check that average works        
        Pair<Double, Ensemble> pair = Pair.of( 1.0, Ensemble.of( 1.5, 2.0 ) );

        assertTrue( EnsembleStatisticsProcessor.getFilterForEnsemblePairs( ThresholdOuter.of( doubles,
                                                                                              condition,
                                                                                              ThresholdOrientation.RIGHT_MEAN ) )
                                               .test( pair ) );
    }

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithoutThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore results = this.getAndCombineStatistics( processors,
                                                                TestDataFactory.getTimeSeriesOfEnsemblePairsOne() );

        DoubleScoreStatisticOuter bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION )
                      .get( 0 );
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
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR )
                      .get( 0 );
        DoubleScoreStatisticOuter rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                      .get( 0 );
        DoubleScoreStatisticOuter crps =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE )
                      .get( 0 );

        assertEquals( -0.032093836077598345,
                      bias.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 9.076475676968208,
                      crps.getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testApplyWithValueThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsOne() );

        //Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.BIAS_FRACTION );
        assertEquals( -0.032093836077598345,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.032093836077598345,
                      bias.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0365931379807274,
                      bias.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.039706682985140816,
                      bias.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0505708024162773,
                      bias.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.056658160809530816,
                      bias.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate CoD
        List<DoubleScoreStatisticOuter> cod = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );

        assertEquals( 0.7873367083297588,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7653639626077698,
                      cod.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.76063213080129,
                      cod.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7542039364210298,
                      cod.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7492338765733539,
                      cod.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate rho
        List<DoubleScoreStatisticOuter> rho = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        assertEquals( 0.8873199582618204,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8748508230594344,
                      rho.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8721422652304439,
                      rho.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.868449155921652,
                      rho.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8655829692024641,
                      rho.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR );

        assertEquals( 11.009512537315405,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 17.675554578575642,
                      mae.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 18.997815872635968,
                      mae.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 20.625668563442147,
                      mae.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 22.094227646773568,
                      mae.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                            MetricConstants.MEAN_ERROR );

        assertEquals( -1.157869354367079,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.1250409720950105,
                      me.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.4855770739425846,
                      me.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -3.4840043925326936,
                      me.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -4.2185439080739515,
                      me.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        assertEquals( 41.01563032408479,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 52.55361580348335,
                      rmse.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 54.82426155439095,
                      rmse.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 58.12352988180837,
                      rmse.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 61.12163959516186,
                      rmse.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testApplyWithValueThresholdsAndCategoricalMeasures()
            throws MetricParameterException, IOException, InterruptedException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<wres.config.yaml.components.Threshold> thresholds = new HashSet<>();
        wres.statistics.generated.Threshold threshold =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( DoubleValue.of( 1.0 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.yaml.components.Threshold thresholdOuter = ThresholdBuilder.builder()
                                                                               .threshold( threshold )
                                                                               .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                                               .build();
        thresholds.add( thresholdOuter );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();
        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( DoubleValue.of( 0.5 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.yaml.components.Threshold oneOuter = ThresholdBuilder.builder()
                                                                         .threshold( one )
                                                                         .type( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                                                         .build();
        Set<wres.config.yaml.components.Threshold> classifiers = Set.of( oneOuter );

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.THREAT_SCORE, null ),
                                      new Metric( MetricConstants.PEIRCE_SKILL_SCORE, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .thresholds( thresholds )
                                                                        .classifierThresholds( classifiers )
                                                                        .ensembleAverageType( wres.statistics.generated.Pool.EnsembleAverageType.MEAN )
                                                                        .minimumSampleSize( 0 )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();
        declaration = DeclarationInterpolator.interpolate( declaration );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsOne() );

        // Obtain the results
        List<DoubleScoreStatisticOuter> actual = statistics.getDoubleScoreStatistics();

        assertEquals( 0.9160756501182034,
                      Slicer.filter( actual, MetricConstants.THREAT_SCORE )
                            .get( 0 )
                            .getComponent( MetricConstants.MAIN )
                            .getStatistic()
                            .getValue(),
                      Precision.EPSILON );

        assertEquals( -0.0012886597938144284,
                      Slicer.filter( actual, MetricConstants.PEIRCE_SKILL_SCORE )
                            .get( 0 )
                            .getComponent( MetricConstants.MAIN )
                            .getStatistic()
                            .getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testApplyWithProbabilityThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithProbabilityThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsOne() );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        //Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION );

        assertEquals( -0.032093836077598345,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.032093836077598345,
                      bias.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0365931379807274,
                      bias.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.039706682985140816,
                      bias.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.05090288343061958,
                      bias.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.056658160809530816,
                      bias.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate CoD
        List<DoubleScoreStatisticOuter> cod =
                Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertEquals( 0.7873367083297588,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7653639626077698,
                      cod.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.76063213080129,
                      cod.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7540690263086123,
                      cod.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7492338765733539,
                      cod.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate rho
        List<DoubleScoreStatisticOuter> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        assertEquals( 0.8873199582618204,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8748508230594344,
                      rho.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8721422652304439,
                      rho.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8683714794421868,
                      rho.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8655829692024641,
                      rho.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR );

        assertEquals( 11.009512537315405,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 17.675554578575642,
                      mae.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 18.997815872635968,
                      mae.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 20.653785159500924,
                      mae.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 22.094227646773568,
                      mae.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( results, MetricConstants.MEAN_ERROR );

        assertEquals( -1.157869354367079,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.1250409720950105,
                      me.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.4855770739425846,
                      me.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -3.5134287820490364,
                      me.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -4.2185439080739515,
                      me.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertEquals( 41.01563032408479,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 52.55361580348335,
                      rmse.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 54.82426155439095,
                      rmse.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 58.191244125990046,
                      rmse.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 61.12163959516186,
                      rmse.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testExceptionOnNullInput() throws MetricParameterException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processor = processors.get( 0 );

        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> processor.apply( null ) );

        assertEquals( "Expected a non-null pool as input to the metric processor.", actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricParameterException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.BRIER_SCORE, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        DeclarationException actual =
                assertThrows( DeclarationException.class,
                              () -> EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration ) );

        assertEquals( "Cannot configure 'BRIER SCORE' without thresholds to define the events: "
                      + "add one or more thresholds to the configuration for the 'BRIER SCORE'.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricParameterException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.BRIER_SCORE, null ) );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.yaml.components.Threshold oneOuter = ThresholdBuilder.builder()
                                                                         .threshold( one )
                                                                         .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                                                         .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .probabilityThresholds( Set.of( oneOuter ) )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        declaration = DeclarationInterpolator.interpolate( declaration );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processor = processors.get( 0 );

        Pool<TimeSeries<Pair<Double, Ensemble>>> pairs = TestDataFactory.getTimeSeriesOfEnsemblePairsThree();

        ThresholdException actual = assertThrows( ThresholdException.class,
                                                  () -> processor.apply( pairs ) );

        assertTrue( actual.getMessage()
                          .startsWith( "Quantiles were required for feature tuple" ) );
    }

    @Test
    public void testApplyWithAllValidMetrics() throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetrics();
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        Set<MetricConstants> actual = processors.stream()
                                                .flatMap( next -> next.getMetrics().stream() )
                                                .collect( Collectors.toSet() );

        // Check for the expected metrics
        Set<MetricConstants> expected = new HashSet<>();
        expected.addAll( SampleDataGroup.ENSEMBLE.getMetrics() );
        expected.addAll( SampleDataGroup.DISCRETE_PROBABILITY.getMetrics() );
        expected.addAll( SampleDataGroup.DICHOTOMOUS.getMetrics() );
        expected.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithValueThresholdsAndMissings()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        StatisticsStore statistics =
                this.getAndCombineStatistics( processors,
                                              TestDataFactory.getTimeSeriesOfEnsemblePairsOneWithMissings() );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        //Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( results, MetricConstants.BIAS_FRACTION );
        assertEquals( -0.032093836077598345,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.032093836077598345,
                      bias.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0365931379807274,
                      bias.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.039706682985140816,
                      bias.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.0505708024162773,
                      bias.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.056658160809530816,
                      bias.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate CoD
        List<DoubleScoreStatisticOuter> cod =
                Slicer.filter( results, MetricConstants.COEFFICIENT_OF_DETERMINATION );
        assertEquals( 0.7873367083297588,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7873367083297588,
                      cod.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7653639626077698,
                      cod.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.76063213080129,
                      cod.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7542039364210298,
                      cod.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.7492338765733539,
                      cod.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate rho
        List<DoubleScoreStatisticOuter> rho =
                Slicer.filter( results, MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        assertEquals( 0.8873199582618204,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8873199582618204,
                      rho.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8748508230594344,
                      rho.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8721422652304439,
                      rho.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.868449155921652,
                      rho.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 0.8655829692024641,
                      rho.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( results, MetricConstants.MEAN_ABSOLUTE_ERROR );

        assertEquals( 11.009512537315405,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 11.009512537315405,
                      mae.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 17.675554578575642,
                      mae.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 18.997815872635968,
                      mae.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 20.625668563442147,
                      mae.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 22.094227646773568,
                      mae.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( results, MetricConstants.MEAN_ERROR );

        assertEquals( -1.157869354367079,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -1.157869354367079,
                      me.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.1250409720950105,
                      me.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -2.4855770739425846,
                      me.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -3.4840043925326936,
                      me.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( -4.2185439080739515,
                      me.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( results, MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        assertEquals( 41.01563032408479,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 41.01563032408479,
                      rmse.get( 1 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 52.55361580348335,
                      rmse.get( 2 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 54.82426155439095,
                      rmse.get( 3 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 58.12352988180837,
                      rmse.get( 4 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
        assertEquals( 61.12163959516186,
                      rmse.get( 5 ).getComponent( MetricConstants.MAIN ).getStatistic().getValue(),
                      Precision.EPSILON );
    }

    @Test
    public void testContingencyTable()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithContingencyTableAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsTwo() );

        // Expected result
        TimeWindow timeWindow = wres.statistics.MessageFactory.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                              Instant.parse( "2010-12-31T11:59:59Z" ),
                                                                              Duration.ofHours( 24 ) );
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( timeWindow );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                      expectedWindow,
                                                                      null,
                                                                      null,
                                                                      false,
                                                                      0L,
                                                                      wres.statistics.generated.Pool.EnsembleAverageType.MEAN );

        PoolMetadata expectedSampleMeta = PoolMetadata.of( evaluation, pool );

        // Obtain the results
        List<DoubleScoreStatisticOuter> results =
                Slicer.filter( statistics.getDoubleScoreStatistics(),
                               meta -> meta.getMetricName().equals( MetricConstants.CONTINGENCY_TABLE )
                                       && meta.getPoolMetadata().getTimeWindow().equals( expectedWindow ) );

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

        Threshold classifierOne = Threshold.newBuilder()
                                           .setLeftThresholdProbability( DoubleValue.of( 0.05 ) )
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setDataType( Threshold.ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter classifierOneWrapped =
                ThresholdOuter.of( classifierOne, wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );

        ThresholdOuter valueThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                           wres.config.yaml.components.ThresholdOperator.GREATER,
                                                           ThresholdOrientation.LEFT,
                                                           MeasurementUnit.of( "CFS" ) );
        OneOrTwoThresholds first = OneOrTwoThresholds.of( valueThreshold,
                                                          classifierOneWrapped );

        PoolMetadata expectedMetaFirst = PoolMetadata.of( expectedSampleMeta, first );

        DoubleScoreStatisticOuter expectedFirst =
                DoubleScoreStatisticOuter.of( firstTable, expectedMetaFirst );

        DoubleScoreStatisticOuter actualFirst =
                Slicer.filter( results, meta -> meta.getPoolMetadata()
                                                    .getThresholds()
                                                    .equals( first ) )
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

        Threshold classifierTwo = Threshold.newBuilder()
                                           .setLeftThresholdProbability( DoubleValue.of( 0.25 ) )
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setDataType( Threshold.ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter classifierTwoWrapped =
                ThresholdOuter.of( classifierTwo, wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );


        OneOrTwoThresholds second = OneOrTwoThresholds.of( valueThreshold,
                                                           classifierTwoWrapped );

        PoolMetadata expectedMetaSecond = PoolMetadata.of( expectedSampleMeta, second );

        DoubleScoreStatisticOuter expectedSecond =
                DoubleScoreStatisticOuter.of( secondTable, expectedMetaSecond );

        DoubleScoreStatisticOuter actualSecond =
                Slicer.filter( results, meta -> meta.getPoolMetadata().getThresholds().equals( second ) )
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

        Threshold classifierThree = Threshold.newBuilder()
                                             .setLeftThresholdProbability( DoubleValue.of( 0.5 ) )
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setDataType( Threshold.ThresholdDataType.LEFT )
                                             .build();
        ThresholdOuter classifierThreeWrapped =
                ThresholdOuter.of( classifierThree, wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );


        OneOrTwoThresholds third = OneOrTwoThresholds.of( valueThreshold,
                                                          classifierThreeWrapped );

        PoolMetadata expectedMetaThird = PoolMetadata.of( expectedSampleMeta, third );

        DoubleScoreStatisticOuter expectedThird =
                DoubleScoreStatisticOuter.of( thirdTable, expectedMetaThird );

        DoubleScoreStatisticOuter actualThird =
                Slicer.filter( results, meta -> meta.getPoolMetadata().getThresholds().equals( third ) )
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

        Threshold classifierFour = Threshold.newBuilder()
                                            .setLeftThresholdProbability( DoubleValue.of( 0.75 ) )
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setDataType( Threshold.ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter classifierFourWrapped =
                ThresholdOuter.of( classifierFour, wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );


        OneOrTwoThresholds fourth = OneOrTwoThresholds.of( valueThreshold,
                                                           classifierFourWrapped );

        PoolMetadata expectedMetaFourth = PoolMetadata.of( expectedSampleMeta, fourth );

        DoubleScoreStatisticOuter expectedFourth =
                DoubleScoreStatisticOuter.of( fourthTable, expectedMetaFourth );
        DoubleScoreStatisticOuter actualFourth =
                Slicer.filter( results, meta -> meta.getPoolMetadata().getThresholds().equals( fourth ) )
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

        Threshold classifierFive = Threshold.newBuilder()
                                            .setLeftThresholdProbability( DoubleValue.of( 0.9 ) )
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setDataType( Threshold.ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter classifierFiveWrapped =
                ThresholdOuter.of( classifierFive
                        , wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );


        OneOrTwoThresholds fifth = OneOrTwoThresholds.of( valueThreshold,
                                                          classifierFiveWrapped );

        PoolMetadata expectedMetaFifth = PoolMetadata.of( expectedSampleMeta, fifth );

        DoubleScoreStatisticOuter expectedFifth =
                DoubleScoreStatisticOuter.of( fifthTable, expectedMetaFifth );

        DoubleScoreStatisticOuter actualFifth =
                Slicer.filter( results, meta -> meta.getPoolMetadata().getThresholds().equals( fifth ) )
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

        Threshold classifierSix = Threshold.newBuilder()
                                           .setLeftThresholdProbability( DoubleValue.of( 0.95 ) )
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setDataType( Threshold.ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter classifierSixWrapped =
                ThresholdOuter.of( classifierSix, wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );

        OneOrTwoThresholds sixth = OneOrTwoThresholds.of( valueThreshold,
                                                          classifierSixWrapped );

        PoolMetadata expectedMetaSixth = PoolMetadata.of( expectedSampleMeta, sixth );

        DoubleScoreStatisticOuter expectedSixth =
                DoubleScoreStatisticOuter.of( sixthTable, expectedMetaSixth );

        DoubleScoreStatisticOuter actualSixth =
                Slicer.filter( results, meta -> meta.getPoolMetadata().getThresholds().equals( sixth ) )
                      .get( 0 );

        assertEquals( expectedSixth, actualSixth );

    }

    @Test
    public void testApplyWithValueThresholdsAndNoData()
            throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsFour() );

        //Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        //Validate the score outputs
        for ( DoubleScoreStatisticOuter nextMetric : results )
        {
            if ( nextMetric.getMetricName() != MetricConstants.SAMPLE_SIZE )
            {
                nextMetric.forEach( next -> assertEquals( Double.NaN, next.getStatistic().getValue(), 0.0 ) );
            }
        }
    }

    @Test
    public void testThatSampleSizeIsConstructedForEnsembleInput() throws MetricParameterException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<Metric> metrics =
                Set.of( new Metric( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE, null ),
                        new Metric( MetricConstants.MEAN_ERROR, null ),
                        new Metric( MetricConstants.SAMPLE_SIZE, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processor = processors.get( 0 );

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
    public void testThatSampleSizeIsConstructedForEnsembleInputWhenProbabilityScoreExists()
            throws MetricParameterException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<Metric> metrics =
                Set.of( new Metric( MetricConstants.BRIER_SCORE, null ),
                        new Metric( MetricConstants.MEAN_ERROR, null ),
                        new Metric( MetricConstants.SAMPLE_SIZE, null ) );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.yaml.components.Threshold oneOuter = ThresholdBuilder.builder()
                                                                         .threshold( one )
                                                                         .type( wres.config.yaml.components.ThresholdType.PROBABILITY )
                                                                         .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .probabilityThresholds( Set.of( oneOuter ) )
                                                                        .metrics( metrics )
                                                                        .build();
        declaration = DeclarationInterpolator.interpolate( declaration );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        Set<MetricConstants> actualSingleValuedScores =
                processors.stream()
                          .flatMap( n -> Arrays.stream( n.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                      StatisticType.DOUBLE_SCORE ) ) )
                          .collect( Collectors.toSet() );

        Set<MetricConstants> expectedSingleValuedScores = Set.of( MetricConstants.MEAN_ERROR );

        assertEquals( expectedSingleValuedScores, actualSingleValuedScores );

        Set<MetricConstants> actualEnsembleScores =
                processors.stream()
                          .flatMap( n -> Arrays.stream( n.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                      StatisticType.DOUBLE_SCORE ) ) )
                          .collect( Collectors.toSet() );

        Set<MetricConstants> expectedEnsembleScores = Set.of( MetricConstants.SAMPLE_SIZE );

        assertEquals( expectedEnsembleScores, actualEnsembleScores );

        Set<MetricConstants> actualProbabilityScores =
                processors.stream()
                          .flatMap( n -> Arrays.stream( n.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY,
                                                                      StatisticType.DOUBLE_SCORE ) ) )
                          .collect( Collectors.toSet() );

        Set<MetricConstants> expectedProbabilityScores = Set.of( MetricConstants.BRIER_SCORE );

        assertEquals( expectedProbabilityScores, actualProbabilityScores );
    }

    /**
     * @param declaration the declaration
     * @return the processors
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
    ofMetricProcessorForEnsemblePairs( EvaluationDeclaration declaration )
    {
        Set<MetricsAndThresholds> metricsAndThresholdsSet =
                ThresholdSlicer.getMetricsAndThresholdsForProcessing( declaration, Set.of()  );
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors = new ArrayList<>();
        for ( MetricsAndThresholds metricsAndThresholds : metricsAndThresholdsSet )
        {
            StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processor
                    = new EnsembleStatisticsProcessor( metricsAndThresholds,
                                                       ForkJoinPool.commonPool(),
                                                       ForkJoinPool.commonPool() );
            processors.add( processor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * Computes and combines the statistics for the supplied processors.
     * @param processors the processors
     * @param pairs the pairs
     * @return the statistics
     */

    private StatisticsStore getAndCombineStatistics( List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors,
                                                     Pool<TimeSeries<Pair<Double, Ensemble>>> pairs )
    {
        StatisticsStore results = null;
        for ( StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> processor : processors )
        {
            StatisticsStore nextResults = processor.apply( pairs );
            if ( Objects.nonNull( results ) )
            {
                results = results.combine( nextResults );
            }
            else
            {
                results = nextResults;
            }
        }
        return results;
    }
}

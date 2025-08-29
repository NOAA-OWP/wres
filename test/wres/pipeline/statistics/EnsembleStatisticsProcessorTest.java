package wres.pipeline.statistics;

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

import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.DeclarationException;
import wres.config.DeclarationInterpolator;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.Metric;
import wres.config.components.MetricBuilder;
import wres.config.components.ThresholdBuilder;
import wres.config.components.ThresholdOrientation;
import wres.config.components.ThresholdType;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.types.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.types.OneOrTwoDoubles;
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
import wres.metrics.singlevalued.BiasFraction;
import wres.metrics.singlevalued.CoefficientOfDetermination;
import wres.metrics.singlevalued.CorrelationPearsons;
import wres.metrics.singlevalued.MeanAbsoluteError;
import wres.metrics.singlevalued.MeanError;
import wres.metrics.singlevalued.RootMeanSquareError;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link EnsembleStatisticsProcessor}.
 *
 * @author James Brown
 */
class EnsembleStatisticsProcessorTest
{
    @Test
    void testGetFilterForEnsemblePairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        wres.config.components.ThresholdOperator condition = wres.config.components.ThresholdOperator.GREATER;
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
    void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
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

        DurationDiagramStatisticOuter diagram = Slicer.filter( results.getDurationDiagramStatistics(),
                                                               MetricConstants.TIME_TO_PEAK_ERROR )
                                                      .get( 0 );

        DurationDiagramStatistic.PairOfInstantAndDuration actual = diagram.getStatistic()
                                                                          .getStatisticsList()
                                                                          .get( 0 );

        Instant expectedInstant = Instant.parse( "1981-12-01T00:00:00Z" );
        Duration expectedDurationHours = Duration.ofHours( 760 );
        com.google.protobuf.Duration expectedDuration = MessageUtilities.getDuration( expectedDurationHours );
        Timestamp expectedTimestamp = MessageUtilities.getTimestamp( expectedInstant );
        DurationDiagramStatistic.PairOfInstantAndDuration expected =
                DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( expectedTimestamp )
                                                                 .setDuration( expectedDuration )
                                                                 .build();

        assertEquals( expected, actual );

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
        assertEquals( -1.1578693543670804,
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
    void testApplyWithValueThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsOne() );

        // Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.BIAS_FRACTION );

        DoubleScoreStatistic bOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.032093836077598345 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.032093836077598345 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.0365931379807274 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.039706682985140816 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.0505708024162773 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.056658160809530816 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> biasExpected = List.of( bOne, bTwo, bThree, bFour, bFive, bSix );
        List<DoubleScoreStatistic> biasActual = bias.stream()
                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                    .toList();
        assertEquals( biasExpected, biasActual );

        // Validate CoD
        List<DoubleScoreStatisticOuter> cod = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );

        DoubleScoreStatistic cOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7873367083297588 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7873367083297588 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7653639626077698 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.76063213080129 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7542039364210298 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7492338765733539 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> codExpected = List.of( cOne, cTwo, cThree, cFour, cFive, cSix );
        List<DoubleScoreStatistic> codActual = cod.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( codExpected, codActual );

        // Validate rho
        List<DoubleScoreStatisticOuter> rho = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        DoubleScoreStatistic rOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8873199582618204 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8873199582618204 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8748508230594344 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8721422652304439 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.868449155921652 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8655829692024641 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> rhoExpected = List.of( rOne, rTwo, rThree, rFour, rFive, rSix );
        List<DoubleScoreStatistic> rhoActual = rho.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( rhoExpected, rhoActual );

        // Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent maeMetric = MeanAbsoluteError.MAIN.toBuilder()
                                                                                       .setUnits( "CMS" )
                                                                                       .build();

        DoubleScoreStatistic mOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 11.009512537315405 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 11.009512537315405 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 17.675554578575646 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 18.997815872635968 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 20.625668563442144 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 22.094227646773568 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> maeExpected = List.of( mOne, mTwo, mThree, mFour, mFive, mSix );
        List<DoubleScoreStatistic> maeActual = mae.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( maeExpected, maeActual );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                            MetricConstants.MEAN_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent meMetric = MeanError.MAIN.toBuilder()
                                                                              .setUnits( "CMS" )
                                                                              .build();

        DoubleScoreStatistic meOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -1.1578693543670804 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -1.1578693543670804 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -2.1250409720950096 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -2.485577073942586 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -3.4840043925326953 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -4.2185439080739515 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> meExpected = List.of( meOne, meTwo, meThree, meFour, meFive, meSix );
        List<DoubleScoreStatistic> meActual = me.stream()
                                                .map( DoubleScoreStatisticOuter::getStatistic )
                                                .toList();
        assertEquals( meExpected, meActual );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent rmseMetric = RootMeanSquareError.MAIN.toBuilder()
                                                                                          .setUnits( "CMS" )
                                                                                          .build();
        DoubleScoreStatistic rmOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 41.01563032408479 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 41.01563032408479 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 52.55361580348335 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 54.824261554390944 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 58.12352988180837 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 61.12163959516187 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> rmseExpected = List.of( rmOne, rmTwo, rmThree, rmFour, rmFive, rmSix );
        List<DoubleScoreStatistic> rmseActual = rmse.stream()
                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                    .toList();
        assertEquals( rmseExpected, rmseActual );
    }

    @Test
    void testApplyWithValueThresholdsAndCategoricalMeasures()
            throws MetricParameterException, IOException, InterruptedException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        Set<wres.config.components.Threshold> thresholds = new HashSet<>();
        wres.statistics.generated.Threshold threshold =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( 1.0 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.components.Threshold thresholdOuter = ThresholdBuilder.builder()
                                                                               .threshold( threshold )
                                                                               .type( wres.config.components.ThresholdType.VALUE )
                                                                               .build();
        thresholds.add( thresholdOuter );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();
        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( 0.5 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.components.Threshold oneOuter = ThresholdBuilder.builder()
                                                                         .threshold( one )
                                                                         .type( wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER )
                                                                         .build();
        Set<wres.config.components.Threshold> classifiers = Set.of( oneOuter );

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
        declaration = DeclarationInterpolator.interpolate( declaration, false );

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
    void testApplyWithProbabilityThresholds()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithProbabilityThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsOne() );

        // Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.BIAS_FRACTION );

        DoubleScoreStatistic bOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.032093836077598345 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.032093836077598345 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.0365931379807274 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.039706682985140816 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.05090288343061958 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.056658160809530816 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> biasExpected = List.of( bOne, bTwo, bThree, bFour, bFive, bSix );
        List<DoubleScoreStatistic> biasActual = bias.stream()
                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                    .toList();
        assertEquals( biasExpected, biasActual );

        // Validate CoD
        List<DoubleScoreStatisticOuter> cod = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );

        DoubleScoreStatistic cOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7873367083297588 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7873367083297588 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7653639626077698 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.76063213080129 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7540690263086123 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7492338765733539 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> codExpected = List.of( cOne, cTwo, cThree, cFour, cFive, cSix );
        List<DoubleScoreStatistic> codActual = cod.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( codExpected, codActual );

        // Validate rho
        List<DoubleScoreStatisticOuter> rho = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        DoubleScoreStatistic rOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8873199582618204 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8873199582618204 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8748508230594344 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8721422652304439 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8683714794421868 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8655829692024641 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> rhoExpected = List.of( rOne, rTwo, rThree, rFour, rFive, rSix );
        List<DoubleScoreStatistic> rhoActual = rho.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( rhoExpected, rhoActual );

        // Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent maeMetric = MeanAbsoluteError.MAIN.toBuilder()
                                                                                       .setUnits( "CMS" )
                                                                                       .build();

        DoubleScoreStatistic mOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 11.009512537315405 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 11.009512537315405 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 17.675554578575646 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 18.997815872635968 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 20.65378515950092 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 22.094227646773568 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> maeExpected = List.of( mOne, mTwo, mThree, mFour, mFive, mSix );
        List<DoubleScoreStatistic> maeActual = mae.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( maeExpected, maeActual );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                            MetricConstants.MEAN_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent meMetric = MeanError.MAIN.toBuilder()
                                                                              .setUnits( "CMS" )
                                                                              .build();

        DoubleScoreStatistic meOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -1.1578693543670804 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -1.1578693543670804 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -2.1250409720950096 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -2.485577073942586 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -3.5134287820490377 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -4.2185439080739515 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> meExpected = List.of( meOne, meTwo, meThree, meFour, meFive, meSix );
        List<DoubleScoreStatistic> meActual = me.stream()
                                                .map( DoubleScoreStatisticOuter::getStatistic )
                                                .toList();
        assertEquals( meExpected, meActual );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent rmseMetric = RootMeanSquareError.MAIN.toBuilder()
                                                                                          .setUnits( "CMS" )
                                                                                          .build();
        DoubleScoreStatistic rmOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 41.01563032408479 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 41.01563032408479 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 52.55361580348335 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 54.824261554390944 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 58.19124412599005 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 61.12163959516187 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> rmseExpected = List.of( rmOne, rmTwo, rmThree, rmFour, rmFive, rmSix );
        List<DoubleScoreStatistic> rmseActual = rmse.stream()
                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                    .toList();
        assertEquals( rmseExpected, rmseActual );
    }

    @Test
    void testExceptionOnNullInput() throws MetricParameterException
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
    void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
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
    void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
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
                                                   .setLeftThresholdProbability( 0.1 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.components.Threshold oneOuter = ThresholdBuilder.builder()
                                                                         .threshold( one )
                                                                         .type( wres.config.components.ThresholdType.PROBABILITY )
                                                                         .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .probabilityThresholds( Set.of( oneOuter ) )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        declaration = DeclarationInterpolator.interpolate( declaration, false );

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
    void testApplyWithAllValidMetrics() throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetrics();
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        Set<MetricConstants> actual = processors.stream()
                                                .flatMap( next -> next.getMetrics()
                                                                      .stream() )
                                                .collect( Collectors.toSet() );

        // Check for the expected metrics
        Set<MetricConstants> expected = new HashSet<>();
        expected.addAll( SampleDataGroup.ENSEMBLE.getMetrics() );
        expected.addAll( SampleDataGroup.DISCRETE_PROBABILITY.getMetrics() );
        expected.addAll( SampleDataGroup.DICHOTOMOUS.getMetrics() );
        expected.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

        // Remove difference metrics, which are not added by default
        expected = expected.stream()
                           .filter( m -> !m.isDifferenceMetric() )
                           .collect( Collectors.toSet() );

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithValueThresholdsAndMissings()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        StatisticsStore statistics =
                this.getAndCombineStatistics( processors,
                                              TestDataFactory.getTimeSeriesOfEnsemblePairsOneWithMissings() );

        // Validate bias
        List<DoubleScoreStatisticOuter> bias = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.BIAS_FRACTION );

        DoubleScoreStatistic bOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.032093836077598345 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.032093836077598345 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.0365931379807274 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.039706682985140816 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.0505708024162773 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic bSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -0.056658160809530816 )
                                                                                 .setMetric( BiasFraction.MAIN ) )
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> biasExpected = List.of( bOne, bTwo, bThree, bFour, bFive, bSix );
        List<DoubleScoreStatistic> biasActual = bias.stream()
                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                    .toList();
        assertEquals( biasExpected, biasActual );

        // Validate CoD
        List<DoubleScoreStatisticOuter> cod = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );

        DoubleScoreStatistic cOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7873367083297588 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7873367083297588 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7653639626077698 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.76063213080129 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7542039364210298 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic cSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.7492338765733539 )
                                                                                 .setMetric( CoefficientOfDetermination.MAIN ) )
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> codExpected = List.of( cOne, cTwo, cThree, cFour, cFive, cSix );
        List<DoubleScoreStatistic> codActual = cod.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( codExpected, codActual );

        // Validate rho
        List<DoubleScoreStatisticOuter> rho = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        DoubleScoreStatistic rOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8873199582618204 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8873199582618204 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8748508230594344 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8721422652304439 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.868449155921652 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.8655829692024641 )
                                                                                 .setMetric( CorrelationPearsons.MAIN ) )
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> rhoExpected = List.of( rOne, rTwo, rThree, rFour, rFive, rSix );
        List<DoubleScoreStatistic> rhoActual = rho.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( rhoExpected, rhoActual );

        // Validate mae
        List<DoubleScoreStatisticOuter> mae = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent maeMetric = MeanAbsoluteError.MAIN.toBuilder()
                                                                                       .setUnits( "CMS" )
                                                                                       .build();

        DoubleScoreStatistic mOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 11.009512537315405 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 11.009512537315405 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 17.675554578575646 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 18.997815872635968 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 20.625668563442144 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic mSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 22.094227646773568 )
                                                                                 .setMetric( maeMetric ) )
                                    .setMetric( MeanAbsoluteError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> maeExpected = List.of( mOne, mTwo, mThree, mFour, mFive, mSix );
        List<DoubleScoreStatistic> maeActual = mae.stream()
                                                  .map( DoubleScoreStatisticOuter::getStatistic )
                                                  .toList();
        assertEquals( maeExpected, maeActual );

        //Validate me
        List<DoubleScoreStatisticOuter> me = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                            MetricConstants.MEAN_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent meMetric = MeanError.MAIN.toBuilder()
                                                                              .setUnits( "CMS" )
                                                                              .build();

        DoubleScoreStatistic meOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -1.1578693543670804 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -1.1578693543670804 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -2.1250409720950096 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -2.485577073942586 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -3.4840043925326953 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic meSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( -4.2185439080739515 )
                                                                                 .setMetric( meMetric ) )
                                    .setMetric( MeanError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> meExpected = List.of( meOne, meTwo, meThree, meFour, meFive, meSix );
        List<DoubleScoreStatistic> meActual = me.stream()
                                                .map( DoubleScoreStatisticOuter::getStatistic )
                                                .toList();
        assertEquals( meExpected, meActual );

        //Validate rmse
        List<DoubleScoreStatisticOuter> rmse = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                                              MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        DoubleScoreMetric.DoubleScoreMetricComponent rmseMetric = RootMeanSquareError.MAIN.toBuilder()
                                                                                          .setUnits( "CMS" )
                                                                                          .build();
        DoubleScoreStatistic rmOne =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 41.01563032408479 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmTwo =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 41.01563032408479 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmThree =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 52.55361580348335 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmFour =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 54.824261554390944 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmFive =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 58.12352988180837 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();
        DoubleScoreStatistic rmSix =
                DoubleScoreStatistic.newBuilder()
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 61.12163959516187 )
                                                                                 .setMetric( rmseMetric ) )
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .build();

        List<DoubleScoreStatistic> rmseExpected = List.of( rmOne, rmTwo, rmThree, rmFour, rmFive, rmSix );
        List<DoubleScoreStatistic> rmseActual = rmse.stream()
                                                    .map( DoubleScoreStatisticOuter::getStatistic )
                                                    .toList();
        assertEquals( rmseExpected, rmseActual );
    }

    @Test
    void testContingencyTable()
            throws IOException, MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithContingencyTableAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );

        Pool<TimeSeries<Pair<Double, Ensemble>>> poolData = TestDataFactory.getTimeSeriesOfEnsemblePairsTwo();
        StatisticsStore statistics = this.getAndCombineStatistics( processors, poolData );

        // Expected result
        TimeWindow timeWindow = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
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
                                           .setLeftThresholdProbability( 0.05 )
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setDataType( Threshold.ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter classifierOneWrapped =
                ThresholdOuter.of( classifierOne, wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER );

        ThresholdOuter valueThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( 50.0 ),
                                                           wres.config.components.ThresholdOperator.GREATER,
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
                                           .setLeftThresholdProbability( 0.25 )
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setDataType( Threshold.ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter classifierTwoWrapped =
                ThresholdOuter.of( classifierTwo, wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER );


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
                                             .setLeftThresholdProbability( 0.5 )
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setDataType( Threshold.ThresholdDataType.LEFT )
                                             .build();
        ThresholdOuter classifierThreeWrapped =
                ThresholdOuter.of( classifierThree, wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER );


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
                                            .setLeftThresholdProbability( 0.75 )
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setDataType( Threshold.ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter classifierFourWrapped =
                ThresholdOuter.of( classifierFour, wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER );


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
                                            .setLeftThresholdProbability( 0.9 )
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setDataType( Threshold.ThresholdDataType.LEFT )
                                            .build();
        ThresholdOuter classifierFiveWrapped =
                ThresholdOuter.of( classifierFive
                        , wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER );


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
                                           .setLeftThresholdProbability( 0.95 )
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setDataType( Threshold.ThresholdDataType.LEFT )
                                           .build();
        ThresholdOuter classifierSixWrapped =
                ThresholdOuter.of( classifierSix, wres.config.components.ThresholdType.PROBABILITY_CLASSIFIER );

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
    void testApplyWithValueThresholdsAndNoData()
            throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsFour() );

        // Obtain the results
        List<DoubleScoreStatisticOuter> results = statistics.getDoubleScoreStatistics();

        // Validate the score outputs
        for ( DoubleScoreStatisticOuter nextMetric : results )
        {
            if ( nextMetric.getMetricName() != MetricConstants.SAMPLE_SIZE )
            {
                nextMetric.forEach( next -> assertEquals( Double.NaN, next.getStatistic().getValue(), 0.0 ) );
            }
        }
    }

    @Test
    void testThatSampleSizeIsConstructedForEnsembleInput() throws MetricParameterException
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
    void testThatSampleSizeIsConstructedForEnsembleInputWhenProbabilityScoreExists()
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
        Features features = FeaturesBuilder.builder()
                                           .geometries( Set.of( geometryTuple ) )
                                           .build();

        wres.statistics.generated.Threshold one =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdProbability( 0.1 )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                   .build();
        wres.config.components.Threshold oneOuter = ThresholdBuilder.builder()
                                                                         .threshold( one )
                                                                         .type( wres.config.components.ThresholdType.PROBABILITY )
                                                                         .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .probabilityThresholds( Set.of( oneOuter ) )
                                                                        .metrics( metrics )
                                                                        .build();
        declaration = DeclarationInterpolator.interpolate( declaration, false );

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

    @Test
    void testSpaghettiPlot() throws InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetrics();

        // Explicitly add a spaghetti plot to override all valid metrics
        wres.config.components.Threshold allData =
                ThresholdBuilder.builder()
                                .threshold( ThresholdOuter.ALL_DATA.getThreshold() )
                                .type( ThresholdType.VALUE )
                                .build();
        declaration = EvaluationDeclarationBuilder.builder( declaration )
                                                  .metrics( Set.of( MetricBuilder.builder()
                                                                                 .name( MetricConstants.SPAGHETTI_PLOT )
                                                                                 .build() ) )
                                                  .thresholds( Set.of( allData ) )
                                                  .build();

        // Interpolate the missing components of the declaration so that it's ready for processing
        declaration = DeclarationInterpolator.interpolate( declaration, false );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EnsembleStatisticsProcessorTest.ofMetricProcessorForEnsemblePairs( declaration );
        StatisticsStore statistics = this.getAndCombineStatistics( processors,
                                                                   TestDataFactory.getTimeSeriesOfEnsemblePairsThree() );

        // Obtain the results
        List<PairsStatisticOuter> actual = statistics.getPairsStatistics();

        PairsMetric metric = PairsMetric.newBuilder()
                                        .setName( MetricName.SPAGHETTI_PLOT )
                                        .setUnits( "MM/DAY" )
                                        .build();

        Timestamp firstTime = Timestamp.newBuilder()
                                       .setSeconds( 479412000 )
                                       .build();
        Timestamp secondTime = Timestamp.newBuilder()
                                        .setSeconds( 479433600 )
                                        .build();

        Pairs.TimeSeriesOfPairs timeSeries =
                Pairs.TimeSeriesOfPairs.newBuilder()
                                       .addReferenceTimes( ReferenceTime.newBuilder()
                                                                        .setReferenceTimeType( ReferenceTime.ReferenceTimeType.T0 )
                                                                        .setReferenceTime( firstTime ) )
                                       .addPairs( Pairs.Pair.newBuilder()
                                                            .addLeft( 22.9 )
                                                            .addRight( 22.8 )
                                                            .addRight( 23.9 )
                                                            .setValidTime( secondTime ) )
                                       .build();

        Pairs pairs = Pairs.newBuilder()
                           .addLeftVariableNames( DatasetOrientation.LEFT.toString()
                                                                         .toUpperCase() )
                           .addRightVariableNames( "MEMBER 1" )
                           .addRightVariableNames( "MEMBER 2" )
                           .addTimeSeries( timeSeries )
                           .build();

        PairsStatistic pairsStatistic = PairsStatistic.newBuilder()
                                                      .setStatistics( pairs )
                                                      .setMetric( metric )
                                                      .build();

        TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                                           Duration.ofHours( 24 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "MAP" )
                                          .setMeasurementUnit( "MM/DAY" )
                                          .build();

        FeatureGroup featureGroup = TestDataFactory.getFeatureGroup();
        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      window,
                                                                      null,
                                                                      OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA ),
                                                                      false );

        PoolMetadata metadata = PoolMetadata.of( PoolMetadata.of( evaluation, pool ),
                                                 wres.statistics.generated.Pool.EnsembleAverageType.MEAN );

        List<PairsStatisticOuter> expected = List.of( PairsStatisticOuter.of( pairsStatistic, metadata ) );

        assertEquals( expected, actual );
    }

    /**
     * @param declaration the declaration
     * @return the processors
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
    ofMetricProcessorForEnsemblePairs( EvaluationDeclaration declaration )
    {
        Set<MetricsAndThresholds> metricsAndThresholdsSet =
                ThresholdSlicer.getMetricsAndThresholdsForProcessing( declaration );
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

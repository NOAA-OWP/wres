package wres.pipeline.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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

import com.google.protobuf.Timestamp;

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
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.*;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.metrics.MetricParameterException;
import wres.metrics.categorical.ContingencyTable;
import wres.metrics.timeseries.TimeToPeakError;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link SingleValuedStatisticsProcessor}.
 *
 * @author James Brown
 */
public final class SingleValuedStatisticsProcessorTest
{
    private static final String DRRC3 = "DRRC3";

    private static final String DRRC2 = "DRRC2";

    /**
     * Streamflow for metadata.
     */

    private static final String STREAMFLOW = "Streamflow";

    /**
     * A date for testing.
     */

    private static final Instant FIRST_DATE = Instant.parse( "1985-01-01T00:00:00Z" );

    /**
     * Another date for testing.
     */

    private static final Instant SECOND_DATE = Instant.parse( "1985-01-02T00:00:00Z" );

    @Test
    public void testApplyWithoutThresholds() throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithoutThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );
        Pool<TimeSeries<Pair<Double, Double>>> pairs = TestDataFactory.getTimeSeriesOfSingleValuedPairsSix();

        StatisticsStore results = this.getAndCombineStatistics( processors, pairs );

        List<DoubleScoreStatisticOuter> bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION );
        List<DoubleScoreStatisticOuter> cod =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.COEFFICIENT_OF_DETERMINATION );
        List<DoubleScoreStatisticOuter> rho =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        List<DoubleScoreStatisticOuter> mae =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ABSOLUTE_ERROR );
        List<DoubleScoreStatisticOuter> me =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR );
        List<DoubleScoreStatisticOuter> rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        List<DoubleScoreStatisticOuter> ve =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.VOLUMETRIC_EFFICIENCY );
        List<BoxplotStatisticOuter> bpe =
                Slicer.filter( results.getBoxPlotStatisticsPerPool(), MetricConstants.BOX_PLOT_OF_ERRORS );

        //Test contents
        assertEquals( 1.6666666666666667,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        assertEquals( 1.0,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 1.0,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 5.0,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 5.0,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 5.0,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.6666666666666666,
                      ve.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        assertEquals( List.of( 5.0, 5.0, 5.0, 5.0, 5.0 ),
                      bpe.get( 0 ).getData().getStatistics( 0 ).getQuantilesList() );
    }

    @Test
    public void testApplyWithThresholds() throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );
        Pool<TimeSeries<Pair<Double, Double>>> pairs = TestDataFactory.getTimeSeriesOfSingleValuedPairsSix();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        // Generate results for 10 nominal lead times
        List<DoubleScoreStatisticOuter> scores = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                             Instant.MAX,
                                                                             Duration.ofHours( i ) );
            TimeWindowOuter window = TimeWindowOuter.of( inner );

            wres.statistics.generated.Pool pool = pairs.getMetadata()
                                                       .getPool()
                                                       .toBuilder()
                                                       .setTimeWindow( window.getTimeWindow() )
                                                       .build();

            PoolMetadata meta = PoolMetadata.of( evaluation, pool );

            Pool<TimeSeries<Pair<Double, Double>>> next =
                    new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addPool( pairs )
                                                                        .setMetadata( meta )
                                                                        .build();

            StatisticsStore statistics = this.getAndCombineStatistics( processors, next );
            scores.addAll( statistics.getDoubleScoreStatistics() );
        }

        // Validate a subset of the data 
        assertEquals( 10, Slicer.filter( scores, MetricConstants.THREAT_SCORE ).size() );

        assertEquals( 20 * 8 + 10,
                      Slicer.filter( scores,
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                         Instant.MAX,
                                                                         Instant.MIN,
                                                                         Instant.MAX,
                                                                         Duration.ofHours( 1 ),
                                                                         Duration.ofHours( 1 ) );
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( inner );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 4.9 ),
                                                          wres.config.yaml.components.ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT,
                                                          MeasurementUnit.of( "CMS" ) ) );

        wres.statistics.generated.Pool pool = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                      expectedWindow,
                                                                      null,
                                                                      expectedThreshold,
                                                                      false );

        PoolMetadata expectedMeta = PoolMetadata.of( evaluation, pool );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 100 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 400 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( scores,
                                                          meta -> meta.getMetadata()
                                                                      .getThresholds()
                                                                      .equals( expectedThreshold )
                                                                  && meta.getMetadata()
                                                                         .getTimeWindow()
                                                                         .equals( expectedWindow )
                                                                  && meta.getMetricName()
                                                                     == MetricConstants.CONTINGENCY_TABLE )
                                                 .get( 0 );

        assertEquals( expected, actual );
    }

    @Test
    public void testForExpectedMetricsWhenAllValidConfigured() throws MetricParameterException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetrics();
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        Set<MetricConstants> actual = processors.stream()
                                                .flatMap( next -> next.getMetrics().stream() )
                                                .collect( Collectors.toSet() );

        // Check for the expected metrics
        Set<MetricConstants> expected = new HashSet<>();
        expected.addAll( SampleDataGroup.DICHOTOMOUS.getMetrics() );
        expected.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesMetrics() throws MetricParameterException, InterruptedException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.TIME_TO_PEAK_ERROR, null ) );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .metrics( metrics )
                                                                        .build();
        declaration = DeclarationInterpolator.interpolate( declaration, true );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        // Break into two time-series to test sequential calls
        Pool<TimeSeries<Pair<Double, Double>>> first = TestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        Pool<TimeSeries<Pair<Double, Double>>> second = TestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        // Compute the metrics
        List<DurationDiagramStatisticOuter> actual = new ArrayList<>();
        StatisticsStore some = this.getAndCombineStatistics( processors, first );
        StatisticsStore more = this.getAndCombineStatistics( processors, second );
        actual.addAll( some.getInstantDurationPairStatistics() );
        actual.addAll( more.getInstantDurationPairStatistics() );

        // Validate the outputs
        // Compare the errors against the benchmark

        // Build the expected output
        // Metadata for the output
        TimeWindow innerFirst = wres.statistics.MessageFactory.getTimeWindow( FIRST_DATE,
                                                                              FIRST_DATE,
                                                                              Duration.ofHours( 6 ),
                                                                              Duration.ofHours( 18 ) );
        TimeWindow innerSecond = wres.statistics.MessageFactory.getTimeWindow( SECOND_DATE,
                                                                               SECOND_DATE,
                                                                               Duration.ofHours( 6 ),
                                                                               Duration.ofHours( 18 ) );

        TimeWindowOuter firstWindow = TimeWindowOuter.of( innerFirst );
        TimeWindowOuter secondWindow = TimeWindowOuter.of( innerSecond );

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          wres.config.yaml.components.ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT_AND_RIGHT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                      firstWindow,
                                                                      null,
                                                                      thresholds,
                                                                      false );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        wres.statistics.generated.Pool poolTwo = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                         secondWindow,
                                                                         null,
                                                                         thresholds,
                                                                         false );

        PoolMetadata m2 = PoolMetadata.of( evaluation, poolTwo );

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_DATE.getEpochSecond() )
                                                                                  .setNanos( FIRST_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -21600 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        DurationDiagramStatistic expectedFirst = DurationDiagramStatistic.newBuilder()
                                                                         .setMetric( TimeToPeakError.METRIC )
                                                                         .addStatistics( one )
                                                                         .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( SECOND_DATE.getEpochSecond() )
                                                                                  .setNanos( SECOND_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds(
                                                                                                                 43200 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        DurationDiagramStatistic expectedSecond = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( TimeToPeakError.METRIC )
                                                                          .addStatistics( two )
                                                                          .build();

        List<DurationDiagramStatisticOuter> expected = new ArrayList<>();
        expected.add( DurationDiagramStatisticOuter.of( expectedFirst, m1 ) );
        expected.add( DurationDiagramStatisticOuter.of( expectedSecond, m2 ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesMetricsWithThresholds() throws MetricParameterException, InterruptedException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.TIME_TO_PEAK_ERROR, null ) );

        FeatureTuple featureTuple = TestDataFactory.getFeatureTuple();
        GeometryTuple geometryTuple = featureTuple.getGeometryTuple();
        Features features = FeaturesBuilder.builder().geometries( Set.of( geometryTuple ) )
                                           .build();

        wres.statistics.generated.Threshold thresholdOne =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( DoubleValue.of( 5.0 ) )
                                                   .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                   .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                                   .build();
        wres.config.yaml.components.Threshold thresholdOneOuter = ThresholdBuilder.builder()
                                                                                  .threshold( thresholdOne )
                                                                                  .type( ThresholdType.VALUE )
                                                                                  .build();
        Set<Threshold> thresholds = Set.of( thresholdOneOuter );
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .features( features )
                                                                        .valueThresholds( thresholds )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();
        declaration = DeclarationInterpolator.interpolate( declaration, true );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        //Break into two time-series to test sequential calls
        Pool<TimeSeries<Pair<Double, Double>>> first = TestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        Pool<TimeSeries<Pair<Double, Double>>> second = TestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        List<DurationDiagramStatisticOuter> actual = new ArrayList<>();
        StatisticsStore some = this.getAndCombineStatistics( processors, first );
        StatisticsStore more = this.getAndCombineStatistics( processors, second );
        actual.addAll( some.getInstantDurationPairStatistics() );
        actual.addAll( more.getInstantDurationPairStatistics() );

        //Validate the outputs
        //Compare the errors against the benchmark

        //Build the expected output
        // Metadata for the output
        TimeWindow innerFirst = wres.statistics.MessageFactory.getTimeWindow( FIRST_DATE,
                                                                              FIRST_DATE,
                                                                              Duration.ofHours( 6 ),
                                                                              Duration.ofHours( 18 ) );
        TimeWindow innerSecond = wres.statistics.MessageFactory.getTimeWindow( SECOND_DATE,
                                                                               SECOND_DATE,
                                                                               Duration.ofHours( 6 ),
                                                                               Duration.ofHours( 18 ) );
        TimeWindowOuter firstWindow = TimeWindowOuter.of( innerFirst );
        TimeWindowOuter secondWindow = TimeWindowOuter.of( innerSecond );

        OneOrTwoThresholds firstThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          wres.config.yaml.components.ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT_AND_RIGHT ) );
        OneOrTwoThresholds secondThreshold = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 5.0 ),
                                                                                       wres.config.yaml.components.ThresholdOperator.GREATER,
                                                                                       ThresholdOrientation.LEFT_AND_RIGHT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      false );

        PoolMetadata source = PoolMetadata.of( evaluation, pool );

        List<DurationDiagramStatisticOuter> expected = new ArrayList<>();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_DATE.getEpochSecond() )
                                                                                  .setNanos( FIRST_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -21600 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        DurationDiagramStatistic expectedFirst = DurationDiagramStatistic.newBuilder()
                                                                         .setMetric( TimeToPeakError.METRIC )
                                                                         .addStatistics( one )
                                                                         .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( SECOND_DATE.getEpochSecond() )
                                                                                  .setNanos( SECOND_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds(
                                                                                                                 43200 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        DurationDiagramStatistic expectedSecond = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( TimeToPeakError.METRIC )
                                                                          .addStatistics( two )
                                                                          .build();

        expected.add( DurationDiagramStatisticOuter.of( expectedFirst,
                                                        PoolMetadata.of( source,
                                                                         firstWindow,
                                                                         firstThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( DurationDiagramStatistic.newBuilder()
                                                                                .setMetric( TimeToPeakError.METRIC )
                                                                                .build(),
                                                        PoolMetadata.of( source,
                                                                         firstWindow,
                                                                         secondThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( expectedSecond,
                                                        PoolMetadata.of( source,
                                                                         secondWindow,
                                                                         firstThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( expectedSecond,
                                                        PoolMetadata.of( source,
                                                                         secondWindow,
                                                                         secondThreshold ) ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesSummaryStats()
            throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithTimeSeriesSummaryStatistics();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        // Break into two time-series to test sequential calls
        Pool<TimeSeries<Pair<Double, Double>>> first = TestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        Pool<TimeSeries<Pair<Double, Double>>> second = TestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        Pool<TimeSeries<Pair<Double, Double>>> aggPool =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( first.get() )
                                                                    .addData( second.get() )
                                                                    .setMetadata( PoolSlicer.unionOf( List.of( first.getMetadata(),
                                                                                                               second.getMetadata() ) ) )
                                                                    .build();

        // Compute the metrics
        StatisticsStore project = this.getAndCombineStatistics( processors, aggPool );

        // Validate the outputs
        // Compare the errors against the benchmark
        List<DurationScoreStatisticOuter> actualScores = project.getDurationScoreStatistics();

        // Metadata
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( FIRST_DATE,
                                                                         SECOND_DATE,
                                                                         Duration.ofHours( 6 ),
                                                                         Duration.ofHours( 18 ) );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          wres.config.yaml.components.ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT_AND_RIGHT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                      timeWindow,
                                                                      null,
                                                                      thresholds,
                                                                      false );

        PoolMetadata scoreMeta = PoolMetadata.of( evaluation, pool );

        com.google.protobuf.Duration expectedMean = wres.statistics.MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMedian = wres.statistics.MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMin = wres.statistics.MessageFactory.parse( Duration.ofHours( -6 ) );
        com.google.protobuf.Duration expectedMax = wres.statistics.MessageFactory.parse( Duration.ofHours( 12 ) );
        com.google.protobuf.Duration expectedMeanAbs = wres.statistics.MessageFactory.parse( Duration.ofHours( 9 ) );

        DurationScoreMetricComponent baseMetric =
                DurationScoreMetricComponent.newBuilder()
                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( Long.MAX_VALUE )
                                                                                     .setNanos( 999_999_999 ) )
                                            .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( 0 ) )
                                            .build();

        DurationScoreMetricComponent meanMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                       .setName( ComponentName.MEAN )
                                                                                       .build();

        DurationScoreMetricComponent medianMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                         .setName( ComponentName.MEDIAN )
                                                                                         .build();

        DurationScoreMetricComponent minMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                      .setName( ComponentName.MINIMUM )
                                                                                      .build();

        DurationScoreMetricComponent maxMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                      .setName( ComponentName.MAXIMUM )
                                                                                      .build();

        DurationScoreMetricComponent meanAbsMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                          .setName( ComponentName.MEAN_ABSOLUTE )
                                                                                          .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds(
                                                                                                                                           0 ) )
                                                                                          .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds(
                                                                                                                                           Long.MAX_VALUE )
                                                                                                                                   .setNanos(
                                                                                                                                           999_999_999 ) )
                                                                                          .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds(
                                                                                                                                           0 ) )
                                                                                          .build();


        DurationScoreStatisticComponent meanComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                       .setMetric( meanMetricComponent )
                                                                                       .setValue( expectedMean )
                                                                                       .build();

        DurationScoreStatisticComponent medianComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                         .setMetric(
                                                                                                 medianMetricComponent )
                                                                                         .setValue( expectedMedian )
                                                                                         .build();

        DurationScoreStatisticComponent minComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( minMetricComponent )
                                                                                      .setValue( expectedMin )
                                                                                      .build();

        DurationScoreStatisticComponent maxComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( maxMetricComponent )
                                                                                      .setValue( expectedMax )
                                                                                      .build();

        DurationScoreStatisticComponent meanAbsComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                          .setMetric(
                                                                                                  meanAbsMetricComponent )
                                                                                          .setValue( expectedMeanAbs )
                                                                                          .build();
        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();
        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .addStatistics( meanComponent )
                                                             .addStatistics( medianComponent )
                                                             .addStatistics( minComponent )
                                                             .addStatistics( maxComponent )
                                                             .addStatistics( meanAbsComponent )
                                                             .build();

        DurationScoreStatisticOuter expectedScoresSource = DurationScoreStatisticOuter.of( score, scoreMeta );
        List<DurationScoreStatisticOuter> expectedScores = new ArrayList<>();
        expectedScores.add( expectedScoresSource );

        assertEquals( expectedScores, actualScores );
    }

    @Test
    public void testApplyWithThresholdsAndNoData() throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        Pool<TimeSeries<Pair<Double, Double>>> pairs = TestDataFactory.getTimeSeriesOfSingleValuedPairsSeven();

        // Generate results for 10 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                             Instant.MAX,
                                                                             Duration.ofHours( i ) );
            TimeWindowOuter window = TimeWindowOuter.of( inner );

            FeatureGroup featureGroup = TestDataFactory.getFeatureGroup();

            Evaluation evaluation = Evaluation.newBuilder()
                                              .setRightVariableName( "SQIN" )
                                              .setRightDataName( "HEFS" )
                                              .setMeasurementUnit( "CMS" )
                                              .build();

            wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                          window,
                                                                          null,
                                                                          null,
                                                                          false );

            PoolMetadata meta = PoolMetadata.of( evaluation, pool );

            Pool<TimeSeries<Pair<Double, Double>>> next =
                    new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( pairs.get() )
                                                                        .setMetadata( meta )
                                                                        .build();

            StatisticsStore statistics = this.getAndCombineStatistics( processors, next );
            assertTrue( statistics.getDoubleScoreStatistics().isEmpty() );
        }
    }

    @Test
    public void testApplyTimeSeriesSummaryStatsWithNoData()
            throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithTimeSeriesSummaryStatistics();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        Pool<TimeSeries<Pair<Double, Double>>> pairs =
                TestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        //Compute the metrics
        StatisticsStore statistics = this.getAndCombineStatistics( processors, pairs );

        //Validate the outputs
        List<DurationScoreStatisticOuter> actualScores = statistics.getDurationScoreStatistics();

        //Metadata
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                         Instant.MAX );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          wres.config.yaml.components.ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT_AND_RIGHT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( TestDataFactory.getFeatureGroup(),
                                                                      timeWindow,
                                                                      null,
                                                                      thresholds,
                                                                      false );

        PoolMetadata scoreMeta = PoolMetadata.of( evaluation, pool );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();
        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .build();

        DurationScoreStatisticOuter expectedScoresSource = DurationScoreStatisticOuter.of( score, scoreMeta );
        List<DurationScoreStatisticOuter> expectedScores = new ArrayList<>();
        expectedScores.add( expectedScoresSource );

        assertEquals( expectedScores, actualScores );
    }

    @Test
    public void testApplyWithMissingPairsForRemoval() throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithThresholds();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        Pool<TimeSeries<Pair<Double, Double>>> pairs = TestDataFactory.getTimeSeriesOfSingleValuedPairsEight();

        // Generate results
        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                         Instant.MAX,
                                                                         Duration.ZERO );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        FeatureGroup featureGroup = TestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "AHPS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      window,
                                                                      null,
                                                                      null,
                                                                      false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Pool<TimeSeries<Pair<Double, Double>>> next =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addPool( pairs )
                                                                    .setMetadata( meta )
                                                                    .build();

        StatisticsStore statistics = this.getAndCombineStatistics( processors, next );

        // Check the sample size
        double size = Slicer.filter( statistics.getDoubleScoreStatistics(),
                                     sampleMeta -> sampleMeta.getMetricName() == MetricConstants.SAMPLE_SIZE
                                                   && !sampleMeta.getMetadata()
                                                                 .getThresholds()
                                                                 .first()
                                                                 .isFinite() )
                            .get( 0 )
                            .getComponent( MetricConstants.MAIN )
                            .getData()
                            .getValue();

        assertEquals( 10.0, size, Precision.EPSILON );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        // Create declaration
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ERROR, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor = processors.get( 0 );

        NullPointerException actual = assertThrows( NullPointerException.class, () -> processor.apply( null ) );

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
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.FREQUENCY_BIAS, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .metrics( metrics ) // All valid
                                                                        .build();

        DeclarationException actual =
                assertThrows( DeclarationException.class,
                              () -> SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs(
                                      declaration ) );

        assertEquals( "Cannot configure 'FREQUENCY BIAS' without thresholds to define the events: "
                      + "add one or more thresholds to the configuration for the 'FREQUENCY BIAS'.",
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
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.THREAT_SCORE, null ) );

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

        declaration = DeclarationInterpolator.interpolate( declaration, true );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( declaration );

        StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor = processors.get( 0 );

        Pool<TimeSeries<Pair<Double, Double>>> pairs = TestDataFactory.getTimeSeriesOfSingleValuedPairsTen();

        ThresholdException actual = assertThrows( ThresholdException.class,
                                                  () -> processor.apply( pairs ) );

        assertTrue( actual.getMessage()
                          .startsWith( "Quantiles were required for feature tuple" ) );
    }

    @Test
    public void testApplyWithThresholdsAndFeatureGroupWithTwoFeatures()
            throws MetricParameterException, InterruptedException
    {
        EvaluationDeclaration declaration =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithThresholds();

        // Remove the parameters because they will be re-interpolated
        Set<Metric> metrics = declaration.metrics()
                                         .stream()
                                         .map( next -> new Metric( next.name(), null ) )
                                         .collect( Collectors.toSet() );

        GeometryTuple drrc2 = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder().setName( DRRC2 ) )
                                           .setRight( Geometry.newBuilder().setName( DRRC2 ) )
                                           .build();

        GeometryTuple drrc3 = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder().setName( DRRC3 ) )
                                           .setRight( Geometry.newBuilder().setName( DRRC3 ) )
                                           .build();

        Set<GeometryTuple> geometries = Set.of( drrc2, drrc3 );
        Features f = FeaturesBuilder.builder()
                                    .geometries( geometries )
                                    .build();
        EvaluationDeclaration finalDeclaration = EvaluationDeclarationBuilder.builder( declaration )
                                                                             .metrics( metrics )
                                                                             .features( f )
                                                                             .build();
        finalDeclaration = DeclarationInterpolator.interpolate( finalDeclaration, true );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                SingleValuedStatisticsProcessorTest.ofMetricProcessorForSingleValuedPairs( finalDeclaration );

        Pool<TimeSeries<Pair<Double, Double>>> pairs = TestDataFactory.getTimeSeriesOfSingleValuedPairsEleven();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        // Add results for 1 nominal lead time of PT3H

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                         Instant.MAX,
                                                                         Duration.ofHours( 3 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        wres.statistics.generated.Pool poolOneDescription = pairs.getMetadata()
                                                                 .getPool()
                                                                 .toBuilder()
                                                                 .setGeometryGroup( GeometryGroup.newBuilder()
                                                                                                 .addGeometryTuples(
                                                                                                         drrc2 ) )
                                                                 .setTimeWindow( window.getTimeWindow() )
                                                                 .build();

        PoolMetadata metaOne = PoolMetadata.of( evaluation, poolOneDescription );

        Pool<TimeSeries<Pair<Double, Double>>> poolOne =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( pairs.get() )
                                                                    .setMetadata( metaOne )
                                                                    .build();

        wres.statistics.generated.Pool poolTwoDescription = pairs.getMetadata()
                                                                 .getPool()
                                                                 .toBuilder()
                                                                 .setGeometryGroup( GeometryGroup.newBuilder()
                                                                                                 .addGeometryTuples(
                                                                                                         drrc3 ) )
                                                                 .setTimeWindow( window.getTimeWindow() )
                                                                 .build();

        PoolMetadata metaTwo = PoolMetadata.of( evaluation, poolTwoDescription );

        Pool<TimeSeries<Pair<Double, Double>>> poolTwo =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( pairs.get() )
                                                                    .setMetadata( metaTwo )
                                                                    .build();

        // Create the combined pool, one for each feature
        Pool<TimeSeries<Pair<Double, Double>>> combinedPool =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addPool( poolOne )
                                                                    .addPool( poolTwo )
                                                                    .build();

        StatisticsStore statistics = this.getAndCombineStatistics( processors, combinedPool );
        List<DoubleScoreStatisticOuter> scores = new ArrayList<>( statistics.getDoubleScoreStatistics() );

        // Validate a subset of the data 
        assertEquals( 1, Slicer.filter( scores, MetricConstants.THREAT_SCORE ).size() );

        assertEquals( 17,
                      Slicer.filter( scores,
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        TimeWindow innerExpected = wres.statistics.MessageFactory.getTimeWindow( Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 Instant.MIN,
                                                                                 Instant.MAX,
                                                                                 Duration.ofHours( 3 ),
                                                                                 Duration.ofHours( 3 ) );
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( innerExpected );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 4.9 ),
                                                          wres.config.yaml.components.ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT,
                                                          MeasurementUnit.of( "CMS" ) ) );

        FeatureGroup groupOne = TestDataFactory.getFeatureGroup( DRRC2 );
        FeatureGroup groupTwo = TestDataFactory.getFeatureGroup( DRRC3 );
        Set<FeatureTuple> features = new HashSet<>();
        features.addAll( groupOne.getFeatures() );
        features.addAll( groupTwo.getFeatures() );
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( features );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      expectedWindow,
                                                                      null,
                                                                      expectedThreshold,
                                                                      false );

        PoolMetadata expectedMeta = PoolMetadata.of( evaluation, pool );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 16 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 2 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 2 ) )
                                    .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( scores,
                                                          meta -> meta.getMetadata()
                                                                      .getThresholds()
                                                                      .equals( expectedThreshold )
                                                                  && meta.getMetadata()
                                                                         .getTimeWindow()
                                                                         .equals( expectedWindow )
                                                                  && meta.getMetricName()
                                                                     == MetricConstants.CONTINGENCY_TABLE )
                                                 .get( 0 );

        assertEquals( expected, actual );
    }

    /**
     * @param declaration the declaration
     * @return the processors
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>>
    ofMetricProcessorForSingleValuedPairs( EvaluationDeclaration declaration )
    {
        Set<MetricsAndThresholds> metricsAndThresholdsSet =
                ThresholdSlicer.getMetricsAndThresholdsForProcessing( declaration );
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors = new ArrayList<>();
        for ( MetricsAndThresholds metricsAndThresholds : metricsAndThresholdsSet )
        {
            StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor
                    = new SingleValuedStatisticsProcessor( metricsAndThresholds,
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

    private StatisticsStore getAndCombineStatistics( List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors,
                                                     Pool<TimeSeries<Pair<Double, Double>>> pairs )
    {
        StatisticsStore results = null;
        for ( StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor : processors )
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

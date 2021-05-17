package wres.vis;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.google.protobuf.Timestamp;

import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;


public abstract class Chart2DTestDataGenerator
{

    private static final FeatureKey NWS_FEATURE = new FeatureKey( "DRRC2", null, null, null );
    private static final FeatureKey USGS_FEATURE =
            new FeatureKey( "09165000", "DOLORES RIVER BELOW RICO, CO.", 4326, "POINT ( -108.0603517 37.63888428 )" );
    private static final FeatureKey NWM_FEATURE = new FeatureKey( "18384141", null, null, null );
    private static final FeatureTuple FEATURE_TUPLE = new FeatureTuple( USGS_FEATURE, NWS_FEATURE, NWM_FEATURE );

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter} comprising the CRPSS for a
     * subset of thresholds and forecast lead times. Reads the input data from
     * {@link #getScalarMetricOutputMapByLeadThreshold()} and slices.
     *
     * @return an output map of verification scores
     */

    public static List<DoubleScoreStatisticOuter> getMetricOutputMapByLeadThresholdOne()
            throws IOException
    {
        List<DoubleScoreStatisticOuter> full = Chart2DTestDataGenerator.getScalarMetricOutputMapByLeadThreshold();
        List<DoubleScoreStatisticOuter> statistics = new ArrayList<>();

        double[][] allow =
                new double[][] { { Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY }, { 0.5, 2707.5 },
                                 { 0.95, 13685.0 }, { 0.99, 26648.0 } };
        for ( final double[] next : allow )
        {
            OneOrTwoThresholds filter =
                    OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( next[1] ),
                                                                               OneOrTwoDoubles.of( next[0] ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );
            Slicer.filter( full, data -> filter.equals( data.getMetadata().getThresholds() ) )
                  .forEach( statistics::add );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter} comprising the CRPSS for a
     * subset of thresholds and forecast lead times. Reads the input data from {@link #getScalarMetricOutputMapByLeadThreshold()}
     * and slices.
     *
     * @return an output map of verification scores
     */
    public static List<DoubleScoreStatisticOuter> getMetricOutputMapByLeadThresholdTwo()
            throws IOException
    {
        List<DoubleScoreStatisticOuter> full = Chart2DTestDataGenerator.getScalarMetricOutputMapByLeadThreshold();
        List<DoubleScoreStatisticOuter> statistics = new ArrayList<>();

        final int[] allow = new int[] { 42, 258, 474, 690 };
        for ( final int next : allow )
        {
            TimeWindowOuter filter = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( next ) );
            Slicer.filter( full, data -> filter.equals( data.getMetadata().getTimeWindow() ) )
                  .forEach( statistics::add );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter}. TODO: add some proper data if we ever make
     * assertions about images. See #58348.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatisticOuter> getScalarMetricOutputMapByLeadThreshold()
            throws IOException
    {
        List<DoubleScoreStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        DoubleScoreStatistic one = DoubleScoreStatistic.newBuilder()
                                                       .setMetric( DoubleScoreMetric.newBuilder()
                                                                                    .setName( MetricName.MEAN_ERROR ) )
                                                       .build();
        DoubleScoreStatisticOuter value =
                DoubleScoreStatisticOuter.of( one, PoolMetadata.of() );

        //Append result
        rawData.add( value );

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter}. TODO: add some proper data if we ever make
     * assertions about images. See #58348.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatisticOuter> getScoreMetricOutputMapByLeadThreshold()
            throws IOException
    {
        List<DoubleScoreStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        DoubleScoreStatistic one = DoubleScoreStatistic.newBuilder()
                                                       .setMetric( DoubleScoreMetric.newBuilder()
                                                                                    .setName( MetricName.MEAN_ERROR ) )
                                                       .build();
        DoubleScoreStatisticOuter value =
                DoubleScoreStatisticOuter.of( one, PoolMetadata.of() );

        //Append result
        rawData.add( value );

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatisticOuter}. TODO: add some proper data if we ever make assertions
     * about images. See #58348.
     *
     * @return an output map of reliability diagrams
     */
    static List<DiagramStatisticOuter> getReliabilityDiagramByLeadThreshold()
            throws IOException
    {
        List<DiagramStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        DiagramStatistic statistic =
                DiagramStatistic.newBuilder()
                                .setMetric( DiagramMetric.newBuilder().setName( MetricName.RELIABILITY_DIAGRAM ) )
                                .build();
        DiagramStatisticOuter value =
                DiagramStatisticOuter.of( statistic, PoolMetadata.of() );

        //Append result
        rawData.add( value );

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatisticOuter}. TODO: add some proper data if we ever make assertions
     * about images. See #58348.
     *
     * @return an output map of ROC diagrams
     */

    static List<DiagramStatisticOuter> getROCDiagramByLeadThreshold()
            throws IOException
    {
        List<DiagramStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        DiagramStatistic rocDiagram = DiagramStatistic.newBuilder()
                                                      .setMetric( DiagramMetric.newBuilder()
                                                                               .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) )
                                                      .build();
        DiagramStatisticOuter value =
                DiagramStatisticOuter.of( rocDiagram, PoolMetadata.of() );

        //Append result
        rawData.add( value );

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatisticOuter}. TODO: add some proper data if we ever make assertions
     * about images. See #58348.
     *
     * @return an output map of rank histograms
     */

    static List<DiagramStatisticOuter> getRankHistogramByLeadThreshold()
            throws IOException
    {
        List<DiagramStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        DiagramStatistic histogram = DiagramStatistic.newBuilder()
                                                     .setMetric( DiagramMetric.newBuilder()
                                                                              .setName( MetricName.RANK_HISTOGRAM ) )
                                                     .build();
        DiagramStatisticOuter value =
                DiagramStatisticOuter.of( histogram, PoolMetadata.of() );

        //Append result
        rawData.add( value );

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatisticOuter}. TODO: add some proper data if we ever make assertions
     * about images. See #58348.
     *
     * @return an output map of QQ diagrams
     */

    static List<DiagramStatisticOuter> getQQDiagramByLeadThreshold()
            throws IOException
    {
        List<DiagramStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        DiagramStatistic qqDiagram = DiagramStatistic.newBuilder()
                                                     .setMetric( DiagramMetric.newBuilder()
                                                                              .setName( MetricName.QUANTILE_QUANTILE_DIAGRAM ) )
                                                     .build();
        DiagramStatisticOuter value =
                DiagramStatisticOuter.of( qqDiagram, PoolMetadata.of() );

        //Append result
        rawData.add( value );

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link BoxplotStatisticOuter}. TODO: add some proper data if we ever make assertions
     * about images. See #58348.
     *
     * @return an output map of verification scores
     */

    static List<BoxplotStatisticOuter> getBoxPlotErrorsByObservedAndLeadThreshold()
            throws IOException
    {
        List<BoxplotStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        BoxplotStatistic boxplot = BoxplotStatistic.newBuilder()
                                                   .setMetric( BoxplotMetric.newBuilder()
                                                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) )
                                                   .build();
        PoolMetadata meta = PoolMetadata.of();

        BoxplotStatisticOuter out = BoxplotStatisticOuter.of( boxplot, meta );

        //Append result
        rawData.add( out );

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link BoxplotStatisticOuter}. TODO: add some proper data if we ever make assertions
     * about images. See #58348.
     *
     * @return an output map of verification scores
     */

    static List<BoxplotStatisticOuter> getBoxPlotErrorsByForecastAndLeadThreshold()
            throws IOException
    {
        List<BoxplotStatisticOuter> rawData = new ArrayList<>();

        // See #58348. There are no useful assertions about this dataset at present. If we develop some, then replace
        // this dataset with something meaningful that does not rely on files being read by the EVS
        BoxplotStatistic boxplot = BoxplotStatistic.newBuilder()
                                                   .setMetric( BoxplotMetric.newBuilder()
                                                                            .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
                                                   .build();
        PoolMetadata metadata = PoolMetadata.of();

        BoxplotStatisticOuter out = BoxplotStatisticOuter.of( boxplot, metadata );

        //Append result
        rawData.add( out );

        //Return the results
        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter} comprising the CRPSS for various
     * rolling time windows at one threshold (all data). Corresponds to the use case in Redmine ticket #40785.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatisticOuter> getScoreOutputForPoolingWindowsFirst()
    {
        final List<DoubleScoreStatisticOuter> rawData = new ArrayList<>();

        // Threshold
        final OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                           OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                           Operator.GREATER,
                                                                           ThresholdDataType.LEFT ) );

        //Source metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "STREAMFLOW" )
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FEATURE_TUPLE,
                                          null,
                                          null,
                                          threshold,
                                          false );

        PoolMetadata source = PoolMetadata.of( evaluation, pool );

        // Rolling window parameters
        Instant start = Instant.parse( "2015-12-01T00:00:00Z" );
        Duration period = Duration.ofDays( 91 );
        Duration frequency = Duration.ofDays( 30 );

        // Source data for the outputs
        double[] sixHourOutputs = new double[] { 0.42, 0.32, 0.54, 0.56, 0.52, 0.82, 0.85, 0.63, 0.79, 0.86 };
        double[] twelveHourOutputs = new double[] { 0.37, 0.29, 0.49, 0.53, 0.49, 0.61, 0.67, 0.59, 0.48, 0.52 };
        double[] eighteenHourOutputs = new double[] { 0.28, 0.2, 0.29, 0.45, 0.36, 0.56, 0.48, 0.42, 0.295, 0.415 };
        double[] twentyFourHourOutputs = new double[] { 0.14, 0.11, 0.13, 0.16, 0.15, 0.2, 0.23, 0.16, 0.22, 0.35 };

        // Iterate through 10 rotations of the frequency
        for ( int i = 0; i < 10; i++ )
        {
            Instant begin = start.plus( frequency.multipliedBy( i ) );
            Instant end = begin.plus( period );
            //Add the 6h data
            TimeWindowOuter sixHourWindow = TimeWindowOuter.of( begin,
                                                                end,
                                                                Duration.ofHours( 6 ) );

            DoubleScoreStatistic sixHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( sixHourOutputs[i] )
                                                                                     .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                           .setName( ComponentName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter sixHourOutput =
                    DoubleScoreStatisticOuter.of( sixHour, PoolMetadata.of( source, sixHourWindow ) );
            rawData.add( sixHourOutput );
            //Add the 12h data
            TimeWindowOuter twelveHourWindow = TimeWindowOuter.of( begin,
                                                                   end,
                                                                   Duration.ofHours( 12 ) );

            DoubleScoreStatistic twelveHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( twelveHourOutputs[i] )
                                                                                     .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                           .setName( ComponentName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter twelveHourOutput =
                    DoubleScoreStatisticOuter.of( twelveHour, PoolMetadata.of( source, twelveHourWindow ) );
            rawData.add( twelveHourOutput );
            //Add the 18h data
            TimeWindowOuter eighteenHourWindow = TimeWindowOuter.of( begin,
                                                                     end,
                                                                     Duration.ofHours( 18 ) );

            DoubleScoreStatistic eighteenHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( eighteenHourOutputs[i] )
                                                                                     .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                           .setName( ComponentName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter eighteenHourOutput =
                    DoubleScoreStatisticOuter.of( eighteenHour, PoolMetadata.of( source, eighteenHourWindow ) );
            rawData.add( eighteenHourOutput );
            //Add the 24h data
            TimeWindowOuter twentyFourHourWindow = TimeWindowOuter.of( begin,
                                                                       end,
                                                                       Duration.ofHours( 24 ) );

            DoubleScoreStatistic twentyFourHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( twentyFourHourOutputs[i] )
                                                                                     .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                           .setName( ComponentName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter twentyFourHourOutput =
                    DoubleScoreStatisticOuter.of( twentyFourHour, PoolMetadata.of( source, twentyFourHourWindow ) );
            rawData.add( twentyFourHourOutput );
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter} comprising the bias fraction
     * for various pooling windows at one threshold (all data). Corresponds to the use case in Redmine ticket #46461.
     *
     * @return an output map of verification scores
     */
    static List<DoubleScoreStatisticOuter> getScoreOutputForPoolingWindowsSecond()
    {
        final List<DoubleScoreStatisticOuter> rawData = new ArrayList<>();

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        //Source metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "STREAMFLOW" )
                                          .setRightDataName( "NWM" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FEATURE_TUPLE,
                                          null,
                                          null,
                                          threshold,
                                          false );

        PoolMetadata source = PoolMetadata.of( evaluation, pool );

        double[] scores = new double[] {
                                         -0.39228763627058233,
                                         -0.38540392640098137,
                                         -0.37290595138891640,
                                         -0.29294118442636000,
                                         -0.21904815321579500,
                                         -0.15832253472025700,
                                         -0.29244152171401800,
                                         -0.28854939865963400,
                                         -0.32666816357502900,
                                         -0.29652842873636000,
                                         -0.28174289655134900,
                                         -0.26014386674719100,
                                         -0.20220839431888500,
                                         -0.26801048204027200,
                                         -0.28350781433349200,
                                         -0.27907401971041900,
                                         -0.25723312071583900,
                                         -0.28349542374488600,
                                         -0.27544986528110100,
                                         -0.25307837568226800,
                                         -0.24993043930250200,
                                         -0.27070337571167200,
                                         -0.25422214821455900,
                                         -0.28105802405674500
        };
        // Build the map
        for ( int i = 0; i < scores.length; i++ )
        {
            String nextDate = "2017-08-08T" + String.format( "%02d", i ) + ":00:00Z";

            TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.parse( nextDate ),
                                                             Instant.parse( nextDate ),
                                                             Duration.ofHours( 0 ),
                                                             Duration.ofHours( 18 ) );

            DoubleScoreStatistic one =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.BIAS_FRACTION ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( scores[i] )
                                                                                     .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                           .setName( ComponentName.MAIN ) ) )
                                        .build();

            rawData.add( DoubleScoreStatisticOuter.of( one, PoolMetadata.of( source, timeWindow ) ) );
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link DurationDiagramStatisticOuter} that comprises a {@link Duration} that represents a time-to-peak error against an
     * {@link Instant} that represents the origin (basis time) of the time-series from which the timing error
     * originates. Contains results for forecasts issued at 12Z each day from 1985-01-01T12:00:00Z to
     * 1985-01-10T12:00:00Z and with a forecast horizon of 336h.
     *
     * @return a paired output of timing errors by basis time
     */

    public static List<DurationDiagramStatisticOuter> getTimeToPeakErrors()
    {

        Instant firstInstant = Instant.parse( "1985-01-01T00:00:00Z" );
        Instant secondInstant = Instant.parse( "1985-01-02T00:00:00Z" );
        Instant thirdInstant = Instant.parse( "1985-01-03T00:00:00Z" );
        Instant fourthInstant = Instant.parse( "1985-01-04T00:00:00Z" );
        Instant fifthInstant = Instant.parse( "1985-01-05T00:00:00Z" );
        Instant sixthInstant = Instant.parse( "1985-01-06T00:00:00Z" );
        Instant seventhInstant = Instant.parse( "1985-01-07T00:00:00Z" );
        Instant eighthInstant = Instant.parse( "1985-01-08T00:00:00Z" );
        Instant ninthInstant = Instant.parse( "1985-01-09T00:00:00Z" );
        Instant tenthInstant = Instant.parse( "1985-01-10T00:00:00Z" );

        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( Long.MIN_VALUE )
                                                                                                     .setNanos( 999_999_999 ) )
                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( 0 ) )
                                                            .build();


        // Create a list of pairs
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        // Add some fake time-to-peak errors to the list
        input.add( Pair.of( Instant.parse( "1985-01-01T12:00:00Z" ), Duration.ofHours( -12 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-02T12:00:00Z" ), Duration.ofHours( -2 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-03T12:00:00Z" ), Duration.ofHours( +2 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-04T12:00:00Z" ), Duration.ofHours( +4 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-05T12:00:00Z" ), Duration.ofHours( +8 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-06T12:00:00Z" ), Duration.ofHours( -12 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-07T12:00:00Z" ), Duration.ofHours( -16 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-08T12:00:00Z" ), Duration.ofHours( -22 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-09T12:00:00Z" ), Duration.ofHours( 0 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-10T12:00:00Z" ), Duration.ofHours( 24 ) ) );


        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( firstInstant.getEpochSecond() )
                                                                                  .setNanos( firstInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -43200 ) )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( secondInstant.getEpochSecond() )
                                                                                  .setNanos( secondInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -7200 ) )
                                                               .build();

        PairOfInstantAndDuration three = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( thirdInstant.getEpochSecond() )
                                                                                    .setNanos( thirdInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds( 7200 ) )
                                                                 .build();

        PairOfInstantAndDuration four = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( fourthInstant.getEpochSecond() )
                                                                                   .setNanos( fourthInstant.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds( 14400 ) )
                                                                .build();

        PairOfInstantAndDuration five = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( fifthInstant.getEpochSecond() )
                                                                                   .setNanos( fifthInstant.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds( 28800 ) )
                                                                .build();

        PairOfInstantAndDuration six = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( sixthInstant.getEpochSecond() )
                                                                                  .setNanos( sixthInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -43200 ) )
                                                               .build();

        PairOfInstantAndDuration seven = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( seventhInstant.getEpochSecond() )
                                                                                    .setNanos( seventhInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds( -57600 ) )
                                                                 .build();

        PairOfInstantAndDuration eight = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( eighthInstant.getEpochSecond() )
                                                                                    .setNanos( eighthInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds( -79200 ) )
                                                                 .build();

        PairOfInstantAndDuration nine = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( ninthInstant.getEpochSecond() )
                                                                                   .setNanos( ninthInstant.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds( 0 ) )
                                                                .build();

        PairOfInstantAndDuration ten = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( tenthInstant.getEpochSecond() )
                                                                                  .setNanos( tenthInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 86400 ) )
                                                               .build();

        DurationDiagramStatistic expectedSource = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( metric )
                                                                          .addStatistics( one )
                                                                          .addStatistics( two )
                                                                          .addStatistics( three )
                                                                          .addStatistics( four )
                                                                          .addStatistics( five )
                                                                          .addStatistics( six )
                                                                          .addStatistics( seven )
                                                                          .addStatistics( eight )
                                                                          .addStatistics( nine )
                                                                          .addStatistics( ten )
                                                                          .build();

        // Create the metadata
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "1985-01-10T00:00:00Z" ),
                                                     Duration.ofHours( 6 ),
                                                     Duration.ofHours( 336 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "Streamflow" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FEATURE_TUPLE,
                                          window,
                                          null,
                                          threshold,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        return Arrays.asList( DurationDiagramStatisticOuter.of( expectedSource, meta ) );
    }

    /**
     * <p>Returns a {@link DurationOutput} that summarizes the time-to-peak errors associated with
     * {@link getTimeToPeakErrors()}. The output includes:</p>
     * <ol>
     * <li>{@link MetricConstants#MEAN}</li>
     * <li>{@link MetricConstants#MEDIAN}</li>
     * <li>{@link MetricConstants#STANDARD_DEVIATION}</li>
     * <li>{@link MetricConstants#MINIMUM}</li>
     * <li>{@link MetricConstants#MAXIMUM}</li>
     * <li>{@link MetricConstants#MEAN_ABSOLUTE}</li>
     * </ol>
     *
     * @return a set of summary statistics for time-to-peak errors
     */

    public static List<DurationScoreStatisticOuter> getTimeToPeakErrorStatistics()
    {
        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "1985-01-10T00:00:00Z" ),
                                                     Duration.ofHours( 6 ),
                                                     Duration.ofHours( 336 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "Streamflow" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FEATURE_TUPLE,
                                          window,
                                          null,
                                          threshold,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                        .build();

        DurationScoreStatistic score =
                DurationScoreStatistic.newBuilder()
                                      .setMetric( metric )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MEAN ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 9360 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MEDIAN ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( -3600 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.STANDARD_DEVIATION ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 48_364 )
                                                                                                                            .setNanos( 615_000_000 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MINIMUM ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( -79_200 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MAXIMUM ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 86_400 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MEAN_ABSOLUTE ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 36_720 ) ) )
                                      .build();

        return Arrays.asList( DurationScoreStatisticOuter.of( score, meta ) );
    }

}

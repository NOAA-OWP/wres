package wres.datamodel.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.EvaluationEvent;
import wres.datamodel.EvaluationEvent.EventType;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.ScaleValidationEvent;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.PairedStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.BoxplotStatistic.Box;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * Tests the {@link MessageFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessageFactoryTest
{
    private static final String ESP = "ESP";
    private static final String HEFS = "HEFS";
    private static final String SQIN = "SQIN";
    private static final Instant TWELFTH_TIME = Instant.parse( "2551-03-20T12:00:00Z" );
    private static final Instant ELEVENTH_TIME = Instant.parse( "2551-03-20T01:00:00Z" );
    private static final Instant TENTH_TIME = Instant.parse( "2551-03-19T12:00:00Z" );
    private static final Instant NINTH_TIME = Instant.parse( "2551-03-19T00:00:00Z" );
    private static final Instant EIGHTH_TIME = Instant.parse( "1985-01-03T01:00:00Z" );
    private static final Instant SEVENTH_TIME = Instant.parse( "1985-01-03T00:00:00Z" );
    private static final Instant SIXTH_TIME = Instant.parse( "1985-01-02T02:00:00Z" );
    private static final Instant FIFTH_TIME = Instant.parse( "1985-01-02T00:00:00Z" );
    private static final Instant FOURTH_TIME = Instant.parse( "1985-01-01T03:00:00Z" );
    private static final Instant THIRD_TIME = Instant.parse( "1985-01-01T02:00:00Z" );
    private static final Instant SECOND_TIME = Instant.parse( "1985-01-01T01:00:00Z" );
    private static final Instant FIRST_TIME = Instant.parse( "1985-01-01T00:00:00Z" );

    private static final Duration EARLIEST_LEAD = Duration.ofHours( 1 );
    private static final Duration LATEST_LEAD = Duration.ofHours( 7 );

    private static final String VARIABLE_NAME = "Streamflow";
    private static final String FEATURE_NAME = "DRRC2";
    private static final MeasurementUnit CMS = MeasurementUnit.of( "CMS" );

    private static final wres.datamodel.time.TimeWindowOuter TIME_WINDOW =
            wres.datamodel.time.TimeWindowOuter.of( NINTH_TIME,
                                                    TENTH_TIME,
                                                    ELEVENTH_TIME,
                                                    TWELFTH_TIME,
                                                    EARLIEST_LEAD,
                                                    LATEST_LEAD );

    /**
     * Scores to serialize.
     */

    private List<DoubleScoreStatisticOuter> scores = null;

    /**
     * Duration scores to serialize.
     */

    private List<DurationScoreStatisticOuter> durationScores = null;

    /**
     * Duration scores to serialize.
     */

    private List<PairedStatisticOuter<Instant, Duration>> durationDiagrams = null;

    /**
     * Diagrams to serialize.
     */

    private List<DiagramStatisticOuter> diagrams = null;

    /**
     * Box plot statistics.
     */

    private List<BoxplotStatisticOuter> boxplots = null;

    /**
     * Pairs to serialize.
     */

    private PoolOfPairs<Double, Ensemble> ensemblePairs = null;

    /**
     * Output directory.
     */

    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    @Before
    public void runBeforeEachTest()
    {
        this.scores = this.getScoreStatisticsForOnePool();
        this.durationScores = this.getDurationScoreStatisticsForOnePool();
        this.diagrams = this.getReliabilityDiagramForOnePool();
        this.durationDiagrams = this.getDurationDiagramStatisticsForOnePool();
        this.boxplots = this.getBoxPlotsForOnePool();
        this.ensemblePairs = this.getPoolOfEnsemblePairs();
    }

    @Test
    public void testCreationOfOneStatisticsMessageWithThreeScoresAndOneDiagram()
            throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsForProject statistics =
                new StatisticsForProject.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                                  .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                                  .build();

        Statistics statisticsOut = MessageFactory.parse( statistics, this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "statistics.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn = null;
        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            statisticsIn = Statistics.parseFrom( stream );
        }

        // JSON string: "String json = com.google.protobuf.util.JsonFormat.printer().print( statisticsIn );"
        assertEquals( statisticsOut, statisticsIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    @Test
    public void testCreationOfTwoStatisticsMessagesEachWithThreeScoresAndOneDiagram()
            throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsForProject statistics =
                new StatisticsForProject.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                                  .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                                  .build();

        Statistics firstOut = MessageFactory.parse( statistics, this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "statistics.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            firstOut.writeDelimitedTo( stream );
            firstOut.writeTo( stream );
        }

        Statistics firstIn = null;
        Statistics secondIn = null;

        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            firstIn = Statistics.parseDelimitedFrom( stream );
            secondIn = Statistics.parseFrom( stream );
        }

        assertEquals( firstOut, firstIn );
        assertEquals( firstOut, secondIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    @Test
    public void testCreationOfOneStatisticsMessageWithTwoBoxPlots() throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsForProject statistics =
                new StatisticsForProject.Builder().addBoxPlotStatisticsPerPair( CompletableFuture.completedFuture( this.boxplots ) )
                                                  .build();

        // Create a statistics message
        Statistics statisticsOut = MessageFactory.parse( statistics,
                                                         this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "box_plot_statistics.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn = null;
        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            statisticsIn = Statistics.parseFrom( stream );
        }

        assertEquals( statisticsOut, statisticsIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    @Test
    public void testCreationOfOneStatisticsMessageWithSeveralDurationScores() throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsForProject statistics =
                new StatisticsForProject.Builder().addDurationScoreStatistics( CompletableFuture.completedFuture( this.durationScores ) )
                                                  .build();

        // Create a statistics message
        Statistics statisticsOut = MessageFactory.parse( statistics );

        Path path = this.outputDirectory.resolve( "duration_score_statistics.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn = null;
        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            statisticsIn = Statistics.parseFrom( stream );
        }

        assertEquals( statisticsOut, statisticsIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    @Test
    public void testCreationOfOneStatisticsMessageWithOneDurationDiagram() throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsForProject statistics =
                new StatisticsForProject.Builder().addInstantDurationPairStatistics( CompletableFuture.completedFuture( this.durationDiagrams ) )
                                                  .build();

        // Create a statistics message
        Statistics statisticsOut = MessageFactory.parse( statistics );

        Path path = this.outputDirectory.resolve( "duration_diagrams_statistics.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn = null;
        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            statisticsIn = Statistics.parseFrom( stream );
        }

        assertEquals( statisticsOut, statisticsIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    @Test
    public void testCreationOfOneEvaluationStatusMessage() throws IOException
    {
        EvaluationEvent warning = ScaleValidationEvent.of( EventType.WARN, "This is a warning event." );
        EvaluationEvent error = ScaleValidationEvent.of( EventType.ERROR, "This is an error event." );
        EvaluationEvent info = ScaleValidationEvent.of( EventType.INFO, "This is an info event." );

        // Create a message
        EvaluationStatus statusOut =
                MessageFactory.parse( ELEVENTH_TIME,
                                      TWELFTH_TIME,
                                      CompletionStatus.COMPLETE_REPORTED_SUCCESS,
                                      List.of( warning, error, info ) );

        Path path = this.outputDirectory.resolve( "status.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statusOut.writeTo( stream );
        }

        EvaluationStatus statusIn = null;
        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            statusIn = EvaluationStatus.parseFrom( stream );
        }

        assertEquals( statusOut, statusIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    @Test
    public void testParseByPool() throws Exception
    {
        // Create a statistics message composed of scores and diagrams
        StatisticsForProject statistics =
                new StatisticsForProject.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                                  .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                                  .build();

        // Scores and diagrams should be split due to threshold difference
        Collection<Statistics> actual = MessageFactory.parseByPool( statistics );

        assertEquals( 2, actual.size() );

        StatisticsForProject scores =
                new StatisticsForProject.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                                  .build();

        StatisticsForProject diagrams =
                new StatisticsForProject.Builder().addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                                  .build();

        Statistics expectedScores = MessageFactory.parse( scores );
        Statistics expectedDiagrams = MessageFactory.parse( diagrams );

        Collection<Statistics> expected = Set.of( expectedDiagrams, expectedScores );

        // Assert set-like equality
        assertTrue( expected.containsAll( actual ) );
        assertTrue( actual.containsAll( expected ) );
    }

    /**
     * Returns a {@link List} containing several {@link DoubleScoreStatisticOuter} for one pool.
     * 
     * @return several score statistics for one pool
     */

    private List<DoubleScoreStatisticOuter> getScoreStatisticsForOnePool()
    {
        wres.datamodel.scale.TimeScaleOuter timeScale =
                wres.datamodel.scale.TimeScaleOuter.of( java.time.Duration.ofHours( 1 ),
                                                        wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );

        Location location = Location.of( (Long) null, FEATURE_NAME, 23.45F, 56.21F, (String) null );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new Builder().setMeasurementUnit( CMS )
                                               .setTimeScale( timeScale )
                                               .setTimeWindow( TIME_WINDOW )
                                               .setThresholds( threshold )
                                               .setIdentifier( datasetIdentifier )
                                               .build();

        StatisticMetadata fakeMetadataA =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        StatisticMetadata fakeMetadataB =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_ERROR,
                                      MetricConstants.MAIN );
        StatisticMetadata fakeMetadataC =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_ABSOLUTE_ERROR,
                                      MetricConstants.MAIN );

        List<DoubleScoreStatisticOuter> fakeOutputs = new ArrayList<>();

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 3.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        fakeOutputs.add( DoubleScoreStatisticOuter.of( one, fakeMetadataA ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( two, fakeMetadataB ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( three, fakeMetadataC ) );

        return Collections.unmodifiableList( fakeOutputs );
    }

    /**
     * Returns a {@link List} containing a {@link DiagramStatisticOuter} that 
     * represents the output of a reliability diagram for one pool.
     * 
     * @return a reliability diagram for one pool
     */

    private List<DiagramStatisticOuter> getReliabilityDiagramForOnePool()
    {
        wres.datamodel.scale.TimeScaleOuter timeScale =
                wres.datamodel.scale.TimeScaleOuter.of( java.time.Duration.ofHours( 1 ),
                                                        wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 11.94128 ),
                                                                                                     OneOrTwoDoubles.of( 0.9 ),
                                                                                                     Operator.GREATER_EQUAL,
                                                                                                     ThresholdDataType.LEFT ) );

        Location location = Location.of( (Long) null, FEATURE_NAME, 23.45F, 56.21F, (String) null );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new Builder().setMeasurementUnit( CMS )
                                               .setTimeScale( timeScale )
                                               .setTimeWindow( TIME_WINDOW )
                                               .setThresholds( threshold )
                                               .setIdentifier( datasetIdentifier )
                                               .build();

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.RELIABILITY_DIAGRAM,
                                      null );

        DiagramMetricComponent forecastComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.FORECAST_PROBABILITY )
                                      .build();

        DiagramMetricComponent observedComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                      .build();

        DiagramMetricComponent sampleComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.SAMPLE_SIZE )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( forecastComponent )
                                            .addComponents( observedComponent )
                                            .addComponents( sampleComponent )
                                            .setName( MetricName.RELIABILITY_DIAGRAM )
                                            .build();

        DiagramStatisticComponent forecastProbability =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.FORECAST_PROBABILITY )
                                         .addAllValues( List.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) )
                                         .build();

        DiagramStatisticComponent observedFrequency =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( List.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) )
                                         .build();

        DiagramStatisticComponent sampleSize =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.SAMPLE_SIZE )
                                         .addAllValues( List.of( 5926.0, 371.0, 540.0, 650.0, 1501.0 ) )
                                         .build();

        DiagramStatistic statistic = DiagramStatistic.newBuilder()
                                                     .addStatistics( forecastProbability )
                                                     .addStatistics( observedFrequency )
                                                     .addStatistics( sampleSize )
                                                     .setMetric( metric )
                                                     .build();

        // Fake output wrapper.
        return Collections.unmodifiableList( List.of( DiagramStatisticOuter.of( statistic, fakeMetadata ) ) );
    }

    /**
     * Returns a {@link List} containing a {@link BoxplotStatisticOuter} for one pool.
     * 
     * @return the box plot statistics for one pool
     */

    private List<BoxplotStatisticOuter> getBoxPlotsForOnePool()
    {
        wres.datamodel.scale.TimeScaleOuter timeScale =
                wres.datamodel.scale.TimeScaleOuter.of( java.time.Duration.ofHours( 1 ),
                                                        wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 11.94128 ),
                                                                                                     OneOrTwoDoubles.of( 0.9 ),
                                                                                                     Operator.GREATER_EQUAL,
                                                                                                     ThresholdDataType.LEFT ) );

        Location location = Location.of( (Long) null, FEATURE_NAME, 23.45F, 56.21F, (String) null );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new Builder().setMeasurementUnit( CMS )
                                               .setTimeScale( timeScale )
                                               .setTimeWindow( TIME_WINDOW )
                                               .setThresholds( threshold )
                                               .setIdentifier( datasetIdentifier )
                                               .build();

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                      null );

        List<Double> probabilities = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .addAllQuantiles( probabilities )
                                            .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
                                            .build();

        BoxplotStatistic boxplotOne = BoxplotStatistic.newBuilder()
                                                      .setMetric( metric )
                                                      .addStatistics( Box.newBuilder()
                                                                         .addAllQuantiles( List.of( 1.0,
                                                                                                    2.0,
                                                                                                    3.0,
                                                                                                    4.0,
                                                                                                    5.0 ) )
                                                                         .setLinkedValue( 11.0 ) )
                                                      .addStatistics( Box.newBuilder()
                                                                         .addAllQuantiles( List.of( 6.0,
                                                                                                    7.0,
                                                                                                    8.0,
                                                                                                    9.0,
                                                                                                    10.0 ) )
                                                                         .setLinkedValue( 22.0 ) )
                                                      .addStatistics( Box.newBuilder()
                                                                         .addAllQuantiles( List.of( 11.0,
                                                                                                    12.0,
                                                                                                    13.0,
                                                                                                    14.0,
                                                                                                    15.0 ) )
                                                                         .setLinkedValue( 33.0 ) )
                                                      .build();

        StatisticMetadata fakeMetadataTwo =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                      null );

        BoxplotMetric metricTwo = BoxplotMetric.newBuilder()
                                               .addAllQuantiles( probabilities )
                                               .setLinkedValueType( LinkedValueType.ENSEMBLE_MEAN )
                                               .build();

        BoxplotStatistic boxplotTwo = BoxplotStatistic.newBuilder()
                                                      .setMetric( metricTwo )
                                                      .addStatistics( Box.newBuilder()
                                                                         .addAllQuantiles( List.of( 16.0,
                                                                                                    17.0,
                                                                                                    18.0,
                                                                                                    19.0,
                                                                                                    20.0 ) )
                                                                         .setLinkedValue( 73.0 ) )
                                                      .addStatistics( Box.newBuilder()
                                                                         .addAllQuantiles( List.of( 21.0,
                                                                                                    22.0,
                                                                                                    23.0,
                                                                                                    24.0,
                                                                                                    25.0 ) )
                                                                         .setLinkedValue( 92.0 ) )
                                                      .addStatistics( Box.newBuilder()
                                                                         .addAllQuantiles( List.of( 26.0,
                                                                                                    27.0,
                                                                                                    28.0,
                                                                                                    29.0,
                                                                                                    30.0 ) )
                                                                         .setLinkedValue( 111.0 ) )
                                                      .build();

        // Fake output wrapper.
        return List.of( BoxplotStatisticOuter.of( boxplotOne, fakeMetadata ),
                        BoxplotStatisticOuter.of( boxplotTwo, fakeMetadataTwo ) );
    }

    private PoolOfPairs<Double, Ensemble> getPoolOfEnsemblePairs()
    {
        PoolOfPairsBuilder<Double, Ensemble> b = new PoolOfPairsBuilder<>();
        SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();

        Instant basisTime = FIRST_TIME;
        values.add( Event.of( SECOND_TIME, Pair.of( 1.0, Ensemble.of( 7.0, 8.0, 9.0 ) ) ) );
        values.add( Event.of( THIRD_TIME, Pair.of( 2.0, Ensemble.of( 10.0, 11.0, 12.0 ) ) ) );
        values.add( Event.of( FOURTH_TIME, Pair.of( 3.0, Ensemble.of( 13.0, 14.0, 15.0 ) ) ) );
        SampleMetadata meta = SampleMetadata.of();
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( basisTime, THIRD_TIME );
        TimeSeries<Pair<Double, Ensemble>> timeSeries = TimeSeries.of( metadata,
                                                                       values );
        b.addTimeSeries( timeSeries ).setMetadata( meta );

        Instant basisTimeTwo = FIFTH_TIME;
        values.clear();
        values.add( Event.of( SIXTH_TIME, Pair.of( 5.0, Ensemble.of( 23.0, 24.0, 25.0 ) ) ) );
        values.add( Event.of( SEVENTH_TIME, Pair.of( 6.0, Ensemble.of( 26.0, 27.0, 28.0 ) ) ) );
        values.add( Event.of( EIGHTH_TIME, Pair.of( 7.0, Ensemble.of( 29.0, 30.0, 31.0 ) ) ) );
        TimeSeriesMetadata metadataTwo = getBoilerplateMetadataWithT0( basisTimeTwo, SEVENTH_TIME );
        TimeSeries<Pair<Double, Ensemble>> timeSeriesTwo = TimeSeries.of( metadataTwo,
                                                                          values );

        b.addTimeSeries( timeSeriesTwo );

        return b.build();
    }

    private List<DurationScoreStatisticOuter> getDurationScoreStatisticsForOnePool()
    {
        TimeWindowOuter timeOne =
                TimeWindowOuter.of( FIRST_TIME,
                                    FIFTH_TIME,
                                    FIRST_TIME,
                                    SEVENTH_TIME,
                                    Duration.ofHours( 1 ),
                                    Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Location location = Location.of( "DOLC2" );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( CMS,
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                      null );

        DurationScoreStatistic score =
                DurationScoreStatistic.newBuilder()
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setName( DurationScoreMetricComponent.ComponentName.MEAN )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 3_600 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setName( DurationScoreMetricComponent.ComponentName.MEDIAN )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 7_200 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setName( DurationScoreMetricComponent.ComponentName.MAXIMUM )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 10_800 ) ) )
                                      .build();

        return Collections.singletonList( DurationScoreStatisticOuter.of( score, fakeMetadata ) );
    }

    private List<PairedStatisticOuter<Instant, Duration>> getDurationDiagramStatisticsForOnePool()
    {
        TimeWindowOuter timeOne =
                TimeWindowOuter.of( FIRST_TIME,
                                    FIFTH_TIME,
                                    FIRST_TIME,
                                    SEVENTH_TIME,
                                    Duration.ofHours( 1 ),
                                    Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Location location = Location.of( "DOLC2" );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( CMS,
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.TIME_TO_PEAK_ERROR,
                                      null );

        List<Pair<Instant, Duration>> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( Pair.of( FIRST_TIME, Duration.ofHours( -2 ) ) );
        fakeOutputs.add( Pair.of( SECOND_TIME, Duration.ofHours( -1 ) ) );
        fakeOutputs.add( Pair.of( THIRD_TIME, Duration.ofHours( 0 ) ) );
        fakeOutputs.add( Pair.of( FOURTH_TIME, Duration.ofHours( 1 ) ) );
        fakeOutputs.add( Pair.of( FIFTH_TIME, Duration.ofHours( 2 ) ) );

        return Collections.singletonList( PairedStatisticOuter.of( fakeOutputs, fakeMetadata ) );
    }

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0, Instant t1 )
    {
        Map<ReferenceTimeType, Instant> times = new TreeMap<>();
        times.put( ReferenceTimeType.T0, t0 );
        times.put( ReferenceTimeType.ISSUED_TIME, t1 );

        return TimeSeriesMetadata.of( times,
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      CMS.getUnit() );
    }

}

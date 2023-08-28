package wres.datamodel.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Timestamp;

import wres.config.MetricConstants;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.SeasonBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.Variable;
import wres.datamodel.Ensemble;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.Evaluation.DefaultData;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
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
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.ValueFilter;

/**
 * Tests the {@link MessageFactory}.
 *
 * @author James Brown
 */

class MessageFactoryTest
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
    private static final Feature FEATURE = Feature.of( wres.statistics.MessageFactory.getGeometry( FEATURE_NAME ) );
    private static final MeasurementUnit CMS = MeasurementUnit.of( "CMS" );

    private static final wres.datamodel.time.TimeWindowOuter TIME_WINDOW =
            wres.datamodel.time.TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( NINTH_TIME,
                                                                                                  TENTH_TIME,
                                                                                                  ELEVENTH_TIME,
                                                                                                  TWELFTH_TIME,
                                                                                                  EARLIEST_LEAD,
                                                                                                  LATEST_LEAD ) );
    private static final Geometry LOCATION = wres.statistics.MessageFactory.getGeometry( FEATURE_NAME,
                                                                                         null,
                                                                                         null,
                                                                                         "POINT ( 23.45, 56.21 )" );
    private static final Geometry ANOTHER_LOCATION = wres.statistics.MessageFactory.getGeometry( "DOLC2" );
    private static final FeatureGroup FEATURE_GROUP =
            FeatureGroup.of( wres.statistics.MessageFactory.getGeometryGroup( null,
                                                                              wres.statistics.MessageFactory.getGeometryTuple(
                                                                                      LOCATION,
                                                                                      LOCATION,
                                                                                      LOCATION ) ) );
    private static final FeatureGroup ANOTHER_FEATURE_GROUP =
            FeatureGroup.of( wres.statistics.MessageFactory.getGeometryGroup( null,
                                                                              wres.statistics.MessageFactory.getGeometryTuple(
                                                                                      ANOTHER_LOCATION,
                                                                                      ANOTHER_LOCATION,
                                                                                      ANOTHER_LOCATION ) ) );

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

    private List<DurationDiagramStatisticOuter> durationDiagrams = null;

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

    private wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> ensemblePairs = null;

    /**
     * Output directory.
     */

    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    @BeforeEach
    void runBeforeEachTest()
    {
        this.scores = this.getScoreStatisticsForOnePool();
        this.durationScores = this.getDurationScoreStatisticsForOnePool();
        this.diagrams = this.getReliabilityDiagramForOnePool();
        this.durationDiagrams = this.getDurationDiagramStatisticsForOnePool();
        this.boxplots = this.getBoxPlotsForOnePool();
        this.ensemblePairs = this.getPoolOfEnsemblePairs();
    }

    @Test
    void testCreationOfOneStatisticsMessageWithThreeScoresAndOneDiagram()
            throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsStore statistics =
                new StatisticsStore.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                             .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                             .build();

        Statistics statisticsOut =
                MessageFactory.parseOnePool( statistics, this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "statistics.pb3" );

        try ( OutputStream stream =
                      Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn;
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
    void testCreationOfTwoStatisticsMessagesEachWithThreeScoresAndOneDiagram()
            throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsStore statistics =
                new StatisticsStore.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                             .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                             .build();

        Statistics firstOut = MessageFactory.parseOnePool( statistics, this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "statistics.pb3" );

        try ( OutputStream stream =
                      Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            firstOut.writeDelimitedTo( stream );
            firstOut.writeTo( stream );
        }

        Statistics firstIn;
        Statistics secondIn;

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
    void testCreationOfOneStatisticsMessageWithTwoBoxPlots() throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsStore statistics =
                new StatisticsStore.Builder().addBoxPlotStatisticsPerPair( CompletableFuture.completedFuture( this.boxplots ) )
                                             .build();

        // Create a statistics message
        Statistics statisticsOut = MessageFactory.parseOnePool( statistics,
                                                                this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "box_plot_statistics.pb3" );

        try ( OutputStream stream =
                      Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn;
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
    void testCreationOfOneStatisticsMessageWithSeveralDurationScores() throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsStore statistics =
                new StatisticsStore.Builder().addDurationScoreStatistics( CompletableFuture.completedFuture( this.durationScores ) )
                                             .build();

        // Create a statistics message
        Statistics statisticsOut = MessageFactory.parseOnePool( statistics );
        Assertions.assertNotNull( statisticsOut );

        Path path = this.outputDirectory.resolve( "duration_score_statistics.pb3" );

        try ( OutputStream stream =
                      Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn;
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
    void testCreationOfOneStatisticsMessageWithOneDurationDiagram() throws IOException, InterruptedException
    {
        // Create a statistics message
        StatisticsStore statistics =
                new StatisticsStore.Builder().addDurationDiagramStatistics( CompletableFuture.completedFuture( this.durationDiagrams ) )
                                             .build();

        // Create a statistics message
        Statistics statisticsOut = MessageFactory.parseOnePool( statistics );
        Assertions.assertNotNull( statisticsOut );

        Path path = this.outputDirectory.resolve( "duration_diagrams_statistics.pb3" );

        try ( OutputStream stream =
                      Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            statisticsOut.writeTo( stream );
        }

        Statistics statisticsIn;
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
    void testGetStatisticsProducesExpectedStatisticsInPoolOrder() throws InterruptedException
    {
        // Create a statistics message composed of scores and diagrams
        StatisticsStore statistics =
                new StatisticsStore.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                             .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                             .build();

        // Scores and diagrams should be split due to threshold difference
        List<Statistics> actual = MessageFactory.getStatistics( statistics );

        assertEquals( 2, actual.size() );

        StatisticsStore scores =
                new StatisticsStore.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                             .build();

        StatisticsStore diagrams =
                new StatisticsStore.Builder().addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                             .build();

        Statistics expectedScores = MessageFactory.parseOnePool( scores );
        Statistics expectedDiagrams = MessageFactory.parseOnePool( diagrams );
        Assertions.assertNotNull( expectedDiagrams );
        Assertions.assertNotNull( expectedScores );

        List<Statistics> expected = List.of( expectedScores, expectedDiagrams );

        // Assert set-like equality
        assertEquals( expected, actual );
    }

    @Test
    void testParseEvaluationDeclarationToEvaluationMessage()
    {
        Dataset observedDataset = DatasetBuilder.builder()
                                                .variable( new Variable( "foo", "fooest" ) )
                                                .label( "fooData" )
                                                .build();

        EnsembleFilter filter = new EnsembleFilter( Set.of( "1923", "1924" ), false );

        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .variable( new Variable( "bar", "barest" ) )
                                                 .label( "barData" )
                                                 .ensembleFilter( filter )
                                                 .build();

        Dataset baselineDataset = DatasetBuilder.builder()
                                                .variable( new Variable( "baz", "bazest" ) )
                                                .label( "bazData" )
                                                .build();

        BaselineDataset baseline = new BaselineDataset( baselineDataset, null, true );

        Season season = SeasonBuilder.builder()
                                     .minimum( MonthDay.of( 1, 1 ) )
                                     .maximum( MonthDay.of( 2, 2 ) )
                                     .build();

        Values values = new Values( 12.1, 23.2, 0.0, 27.0 );

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ),
                                      new Metric( MetricConstants.MEAN_ERROR, null ),
                                      new Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, null ) );

        Outputs.NumericFormat numericFormat
                = Outputs.NumericFormat.newBuilder()
                                       .setDecimalFormat( "#0.00" )
                                       .build();
        Outputs.GraphicFormat graphicFormat
                = Outputs.GraphicFormat.newBuilder()
                                       .setWidth( 1200 )
                                       .setHeight( 800 )
                                       .setShape( Outputs.GraphicFormat.GraphicShape.THRESHOLD_LEAD )
                                       .build();

        Outputs outputs = Outputs.newBuilder()
                                 .setNetcdf2( Outputs.Netcdf2Format.getDefaultInstance() )
                                 .setPairs( Outputs.PairFormat.newBuilder()
                                                              .setOptions( numericFormat )
                                                              .build() )
                                 .setCsv2( Outputs.Csv2Format.newBuilder()
                                                             .setOptions( numericFormat )
                                                             .build() )
                                 .setCsv( Outputs.CsvFormat.newBuilder()
                                                           .setOptions( numericFormat )
                                                           .build() )
                                 .setPng( Outputs.PngFormat.newBuilder()
                                                           .setOptions( graphicFormat )
                                                           .build() )
                                 .build();
        Formats formats = new Formats( outputs );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( observedDataset )
                                                                       .right( predictedDataset )
                                                                       .baseline( baseline )
                                                                       .unit( "qux" )
                                                                       .metrics( metrics )
                                                                       .season( season )
                                                                       .values( values )
                                                                       .formats( formats )
                                                                       .build();

        Evaluation actual = MessageFactory.parse( evaluation );

        // Build the expectation
        wres.statistics.generated.Season seasonFilter = wres.statistics.generated.Season.newBuilder()
                                                                                        .setStartDay( 1 )
                                                                                        .setStartMonth( 1 )
                                                                                        .setEndDay( 2 )
                                                                                        .setEndMonth( 2 )
                                                                                        .build();

        ValueFilter valueFilter = ValueFilter.newBuilder()
                                             .setMinimumInclusiveValue( 12.1 )
                                             .setMaximumInclusiveValue( 23.2 )
                                             .build();

        Evaluation expected = Evaluation.newBuilder()
                                        .setLeftDataName( "fooData" )
                                        .setRightDataName( "barData" )
                                        .setDefaultBaseline( DefaultData.OBSERVED_CLIMATOLOGY )
                                        .setBaselineDataName( "bazData" )
                                        .setLeftVariableName( "fooest" )
                                        .setRightVariableName( "barest" )
                                        .setBaselineVariableName( "bazest" )
                                        .setMeasurementUnit( "qux" )
                                        .setSeason( seasonFilter )
                                        .setValueFilter( valueFilter )
                                        .setOutputs( outputs )
                                        .addAllEnsembleMemberSubset( List.of( "1923", "1924" ) )
                                        .setMetricCount( 3 )
                                        .build();

        assertEquals( expected, actual );
    }

    @Test
    void testParseEvaluationDeclarationWithPersistenceToEvaluationMessage()
    {
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baseline = new BaselineDataset( baselineDataset, persistence, true );
        Formats formats = new Formats( Outputs.getDefaultInstance() );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( DatasetBuilder.builder()
                                                                                            .build() )
                                                                       .right( DatasetBuilder.builder()
                                                                                             .build() )
                                                                       .baseline( baseline )
                                                                       .formats( formats )
                                                                       .build();

        Evaluation actual = MessageFactory.parse( evaluation );

        Evaluation expected = Evaluation.newBuilder()
                                        .setDefaultBaseline( DefaultData.OBSERVED_CLIMATOLOGY )
                                        .setBaselineDataName( "PERSISTENCE" )
                                        .setOutputs( Outputs.getDefaultInstance() )
                                        .build();

        assertEquals( expected, actual );
    }

    @Test
    void testParseEvaluationDeclarationWithImplicitBaselineToEvaluationMessage()
    {
        Formats formats = new Formats( Outputs.getDefaultInstance() );
        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR_SKILL_SCORE, null ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( DatasetBuilder.builder()
                                                                                            .build() )
                                                                       .right( DatasetBuilder.builder()
                                                                                             .build() )
                                                                       .metrics( metrics )
                                                                       .formats( formats )
                                                                       .build();

        Evaluation actual = MessageFactory.parse( evaluation );

        Evaluation expected = Evaluation.newBuilder()
                                        .setDefaultBaseline( DefaultData.OBSERVED_CLIMATOLOGY )
                                        .setBaselineDataName( "OBSERVED CLIMATOLOGY" )
                                        .setOutputs( Outputs.getDefaultInstance() )
                                        .setMetricCount( 1 )
                                        .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGetGeometryGroupProducesGroupWithPredictableOrderOfTuples()
    {
        GeometryTuple ga = GeometryTuple.newBuilder()
                                        .setLeft( Geometry.newBuilder().setName( "a" ) )
                                        .setRight( Geometry.newBuilder().setName( "a" ) )
                                        .setBaseline( Geometry.newBuilder().setName( "a" ) )
                                        .build();
        FeatureTuple ta = FeatureTuple.of( ga );

        GeometryTuple gb = GeometryTuple.newBuilder()
                                        .setLeft( Geometry.newBuilder().setName( "b" ) )
                                        .setRight( Geometry.newBuilder().setName( "b" ) )
                                        .setBaseline( Geometry.newBuilder().setName( "b" ) )
                                        .build();
        FeatureTuple tb = FeatureTuple.of( gb );

        GeometryTuple gc = GeometryTuple.newBuilder()
                                        .setLeft( Geometry.newBuilder().setName( "c" ) )
                                        .setRight( Geometry.newBuilder().setName( "c" ) )
                                        .setBaseline( Geometry.newBuilder().setName( "c" ) )
                                        .build();
        FeatureTuple tc = FeatureTuple.of( gc );

        Set<FeatureTuple> unsortedSet = new HashSet<>();
        unsortedSet.add( ta );
        unsortedSet.add( tb );
        unsortedSet.add( tc );

        Set<FeatureTuple> sortedSet = new TreeSet<>();
        sortedSet.add( ta );
        sortedSet.add( tb );
        sortedSet.add( tc );

        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( unsortedSet );
        GeometryGroup geoGroupTwo = MessageFactory.getGeometryGroup( sortedSet );

        assertEquals( geoGroup, geoGroupTwo );
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
                                                        TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                                    ThresholdOperator.GREATER,
                                                                                    ThresholdOrientation.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setBaselineDataName( ESP )
                                          .setMeasurementUnit( CMS.toString() )
                                          .build();

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            TIME_WINDOW,
                                            timeScale,
                                            threshold,
                                            false,
                                            1 );

        PoolMetadata metadata = PoolMetadata.of( evaluation, pool );

        List<DoubleScoreStatisticOuter> fakeOutputs = new ArrayList<>();

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 3.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        fakeOutputs.add( DoubleScoreStatisticOuter.of( one, metadata ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( two, metadata ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( three, metadata ) );

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
                                                        TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of(
                                                                                                             11.94128 ),
                                                                                                     OneOrTwoDoubles.of(
                                                                                                             0.9 ),
                                                                                                     ThresholdOperator.GREATER_EQUAL,
                                                                                                     ThresholdOrientation.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setBaselineDataName( ESP )
                                          .setMeasurementUnit( CMS.toString() )
                                          .build();

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            TIME_WINDOW,
                                            timeScale,
                                            threshold,
                                            false,
                                            1 );

        PoolMetadata metadata = PoolMetadata.of( evaluation, pool );

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
                                            .setName( MetricName.RELIABILITY_DIAGRAM )
                                            .build();

        DiagramStatisticComponent forecastProbability =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( forecastComponent )
                                         .addAllValues( List.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) )
                                         .build();

        DiagramStatisticComponent observedFrequency =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( observedComponent )
                                         .addAllValues( List.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) )
                                         .build();

        DiagramStatisticComponent sampleSize =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( sampleComponent )
                                         .addAllValues( List.of( 5926.0, 371.0, 540.0, 650.0, 1501.0 ) )
                                         .build();

        DiagramStatistic statistic = DiagramStatistic.newBuilder()
                                                     .addStatistics( forecastProbability )
                                                     .addStatistics( observedFrequency )
                                                     .addStatistics( sampleSize )
                                                     .setMetric( metric )
                                                     .build();

        // Fake output wrapper.
        return List.of( DiagramStatisticOuter.of( statistic, metadata ) );
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
                                                        TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of(
                                                                                                             11.94128 ),
                                                                                                     OneOrTwoDoubles.of(
                                                                                                             0.9 ),
                                                                                                     ThresholdOperator.GREATER_EQUAL,
                                                                                                     ThresholdOrientation.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setBaselineDataName( ESP )
                                          .setMeasurementUnit( CMS.toString() )
                                          .build();

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            TIME_WINDOW,
                                            timeScale,
                                            threshold,
                                            false,
                                            1 );

        PoolMetadata metadata = PoolMetadata.of( evaluation, pool );

        List<Double> probabilities = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
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

        BoxplotMetric metricTwo = BoxplotMetric.newBuilder()
                                               .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
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
        return List.of( BoxplotStatisticOuter.of( boxplotOne, metadata ),
                        BoxplotStatisticOuter.of( boxplotTwo, metadata ) );
    }

    private wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> getPoolOfEnsemblePairs()
    {
        wres.datamodel.pools.Pool.Builder<TimeSeries<Pair<Double, Ensemble>>> b =
                new wres.datamodel.pools.Pool.Builder<>();
        SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();

        values.add( Event.of( SECOND_TIME, Pair.of( 1.0, Ensemble.of( 7.0, 8.0, 9.0 ) ) ) );
        values.add( Event.of( THIRD_TIME, Pair.of( 2.0, Ensemble.of( 10.0, 11.0, 12.0 ) ) ) );
        values.add( Event.of( FOURTH_TIME, Pair.of( 3.0, Ensemble.of( 13.0, 14.0, 15.0 ) ) ) );
        PoolMetadata meta = PoolMetadata.of();
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( FIRST_TIME, THIRD_TIME );
        TimeSeries<Pair<Double, Ensemble>> timeSeries = TimeSeries.of( metadata,
                                                                       values );
        b.addData( timeSeries ).setMetadata( meta );

        values.clear();
        values.add( Event.of( SIXTH_TIME, Pair.of( 5.0, Ensemble.of( 23.0, 24.0, 25.0 ) ) ) );
        values.add( Event.of( SEVENTH_TIME, Pair.of( 6.0, Ensemble.of( 26.0, 27.0, 28.0 ) ) ) );
        values.add( Event.of( EIGHTH_TIME, Pair.of( 7.0, Ensemble.of( 29.0, 30.0, 31.0 ) ) ) );
        TimeSeriesMetadata metadataTwo = getBoilerplateMetadataWithT0( FIFTH_TIME, SEVENTH_TIME );
        TimeSeries<Pair<Double, Ensemble>> timeSeriesTwo = TimeSeries.of( metadataTwo,
                                                                          values );

        b.addData( timeSeriesTwo );

        return b.build();
    }

    private List<DurationScoreStatisticOuter> getDurationScoreStatisticsForOnePool()
    {
        TimeWindowOuter timeOne = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( FIRST_TIME,
                                                                                                    FIFTH_TIME,
                                                                                                    FIRST_TIME,
                                                                                                    SEVENTH_TIME,
                                                                                                    Duration.ofHours( 1 ),
                                                                                                    Duration.ofHours( 18 ) ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setBaselineDataName( ESP )
                                          .setMeasurementUnit( CMS.toString() )
                                          .build();

        Pool pool = MessageFactory.getPool( ANOTHER_FEATURE_GROUP,
                                            timeOne,
                                            null,
                                            threshold,
                                            false,
                                            1 );

        PoolMetadata fakeMetadata = PoolMetadata.of( evaluation, pool );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                        .build();

        DurationScoreStatistic score =
                DurationScoreStatistic.newBuilder()
                                      .setMetric( metric )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 DurationScoreMetricComponent.ComponentName.MEAN ) )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    3_600 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 DurationScoreMetricComponent.ComponentName.MEDIAN ) )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    7_200 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 DurationScoreMetricComponent.ComponentName.MAXIMUM ) )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    10_800 ) ) )
                                      .build();

        return Collections.singletonList( DurationScoreStatisticOuter.of( score, fakeMetadata ) );
    }

    private List<DurationDiagramStatisticOuter> getDurationDiagramStatisticsForOnePool()
    {
        TimeWindowOuter timeOne = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow( FIRST_TIME,
                                                                                                    FIFTH_TIME,
                                                                                                    FIRST_TIME,
                                                                                                    SEVENTH_TIME,
                                                                                                    Duration.ofHours( 1 ),
                                                                                                    Duration.ofHours( 18 ) ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setBaselineDataName( ESP )
                                          .setMeasurementUnit( CMS.toString() )
                                          .build();

        Pool pool = MessageFactory.getPool( ANOTHER_FEATURE_GROUP,
                                            timeOne,
                                            null,
                                            threshold,
                                            false,
                                            1 );

        PoolMetadata fakeMetadata = PoolMetadata.of( evaluation, pool );

        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .build();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_TIME.getEpochSecond() )
                                                                                  .setNanos( FIRST_TIME.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -7200 ) )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( SECOND_TIME.getEpochSecond() )
                                                                                  .setNanos( SECOND_TIME.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -3600 ) )
                                                               .build();

        PairOfInstantAndDuration three = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( THIRD_TIME.getEpochSecond() )
                                                                                    .setNanos( THIRD_TIME.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds(
                                                                                                                   0 ) )
                                                                 .build();

        PairOfInstantAndDuration four = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( FOURTH_TIME.getEpochSecond() )
                                                                                   .setNanos( FOURTH_TIME.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds(
                                                                                                                  3600 ) )
                                                                .build();

        PairOfInstantAndDuration five = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( FIFTH_TIME.getEpochSecond() )
                                                                                   .setNanos( FIFTH_TIME.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds(
                                                                                                                  7200 ) )
                                                                .build();

        DurationDiagramStatistic statistic = DurationDiagramStatistic.newBuilder()
                                                                     .setMetric( metric )
                                                                     .addStatistics( one )
                                                                     .addStatistics( two )
                                                                     .addStatistics( three )
                                                                     .addStatistics( four )
                                                                     .addStatistics( five )
                                                                     .build();

        return Collections.singletonList( DurationDiagramStatisticOuter.of( statistic, fakeMetadata ) );
    }

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0, Instant t1 )
    {
        Map<ReferenceTimeType, Instant> times = new TreeMap<>();
        times.put( ReferenceTimeType.T0, t0 );
        times.put( ReferenceTimeType.ISSUED_TIME, t1 );

        return TimeSeriesMetadata.of( times,
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE,
                                      CMS.getUnit() );
    }

}

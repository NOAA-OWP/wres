package wres.statistics;

import static org.junit.Assert.assertEquals;

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
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Timestamp;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Statistics;

/**
 * Tests the {@link StatisticsMessageCreator}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticsMessageCreatorTest
{

    private static final Instant EIGHTH_TIME = Instant.parse( "1985-01-03T01:00:00Z" );
    private static final Instant SEVENTH_TIME = Instant.parse( "1985-01-03T00:00:00Z" );
    private static final Instant SIXTH_TIME = Instant.parse( "1985-01-02T02:00:00Z" );
    private static final Instant FIFTH_TIME = Instant.parse( "1985-01-02T00:00:00Z" );
    private static final Instant FOURTH_TIME = Instant.parse( "1985-01-01T03:00:00Z" );
    private static final Instant THIRD_TIME = Instant.parse( "1985-01-01T02:00:00Z" );
    private static final Instant SECOND_TIME = Instant.parse( "1985-01-01T01:00:00Z" );
    private static final Instant FIRST_TIME = Instant.parse( "1985-01-01T00:00:00Z" );

    private static final String VARIABLE_NAME = "Streamflow";
    private static final String FEATURE_NAME = "DRRC2";
    private static final String UNIT = "CMS";

    /**
     * Scores to serialize.
     */

    private List<DoubleScoreStatistic> scores = null;

    /**
     * Diagrams to serialize.
     */

    private List<DiagramStatistic> diagrams = null;

    /**
     * Pairs to serialize.
     */

    private PoolOfPairs<Double, Ensemble> ensemblePairs = null;

    /**
     * Minimal representation of an evaluation.
     */

    private Evaluation evaluation = null;

    /**
     * Output directory.
     */

    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    @Before
    public void runBeforeEachTest()
    {
        this.scores = this.getScoreStatisticsForOnePool();
        this.diagrams = this.getReliabilityDiagramForOnePool();
        this.ensemblePairs = this.getPoolOfEnsemblePairs();
        this.evaluation = this.createEvaluation();
    }

    @Test
    public void testCreationOfOneStatisticsMessageWithThreeScoresAndOneDiagram() throws IOException
    {
        // Create a statistics message
        Statistics statisticsOut =
                StatisticsMessageCreator.parse( this.evaluation, this.scores, this.diagrams, this.ensemblePairs );

        Path path = outputDirectory.resolve( "statistics.pb3" );

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
    public void testCreationOfTwoStatisticsMessagesEachWithThreeScoresAndOneDiagram() throws IOException
    {
        // Create two statistics message, mostly with the same payload, but with a different job identifier
        Statistics firstOut =
                StatisticsMessageCreator.parse( this.evaluation, this.scores, this.diagrams, this.ensemblePairs );

        Evaluation evaluationTwo =
                Evaluation.newBuilder( this.evaluation ).setJobIdentifier( "14187222026701703271" ).build();

        Statistics secondOut =
                StatisticsMessageCreator.parse( evaluationTwo, this.scores, this.diagrams, this.ensemblePairs );

        Path path = outputDirectory.resolve( "statistics.pb3" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            firstOut.writeDelimitedTo( stream );
            secondOut.writeTo( stream );
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
        assertEquals( secondOut, secondIn );

        // Delete if succeeded
        Files.deleteIfExists( path );
    }

    /**
     * Returns a {@link List} containing several {@link DoubleScoreStatistic} for one pool.
     * 
     * @return several score statistics for one pool
     */

    private List<DoubleScoreStatistic> getScoreStatisticsForOnePool()
    {

        Instant earliestValidTime = Instant.parse( "2551-03-20T01:00:00Z" );
        Instant latestValidTime = Instant.parse( "2551-03-20T12:00:00Z" );
        Instant earliestReferenceTime = Instant.parse( "2551-03-19T00:00:00Z" );
        Instant latestReferenceTime = Instant.parse( "2551-03-19T12:00:00Z" );
        java.time.Duration earliestLead = java.time.Duration.ofHours( 1 );
        java.time.Duration latestLead = java.time.Duration.ofHours( 7 );

        wres.datamodel.time.TimeWindow timeWindow = wres.datamodel.time.TimeWindow.of( earliestReferenceTime,
                                                                                       latestReferenceTime,
                                                                                       earliestValidTime,
                                                                                       latestValidTime,
                                                                                       earliestLead,
                                                                                       latestLead );

        wres.datamodel.scale.TimeScale timeScale =
                wres.datamodel.scale.TimeScale.of( java.time.Duration.ofHours( 1 ),
                                                   wres.datamodel.scale.TimeScale.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );

        Location location = Location.of( (Long) null, FEATURE_NAME, 23.45F, 56.21F, (String) null );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                             .setTimeScale( timeScale )
                                                             .setTimeWindow( timeWindow )
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

        List<DoubleScoreStatistic> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( DoubleScoreStatistic.of( 1.0, fakeMetadataA ) );
        fakeOutputs.add( DoubleScoreStatistic.of( 2.0, fakeMetadataB ) );
        fakeOutputs.add( DoubleScoreStatistic.of( 3.0, fakeMetadataC ) );

        return Collections.unmodifiableList( fakeOutputs );
    }

    /**
     * Returns a {@link List} containing a {@link DiagramStatistic} that 
     * represents the output of a reliability diagram for one pool.
     * 
     * @return a reliability diagram for one pool
     */

    private List<DiagramStatistic> getReliabilityDiagramForOnePool()
    {

        Instant earliestValidTime = Instant.parse( "2551-03-20T01:00:00Z" );
        Instant latestValidTime = Instant.parse( "2551-03-20T12:00:00Z" );
        Instant earliestReferenceTime = Instant.parse( "2551-03-19T00:00:00Z" );
        Instant latestReferenceTime = Instant.parse( "2551-03-19T12:00:00Z" );
        java.time.Duration earliestLead = java.time.Duration.ofHours( 1 );
        java.time.Duration latestLead = java.time.Duration.ofHours( 7 );

        wres.datamodel.time.TimeWindow timeWindow = wres.datamodel.time.TimeWindow.of( earliestReferenceTime,
                                                                                       latestReferenceTime,
                                                                                       earliestValidTime,
                                                                                       latestValidTime,
                                                                                       earliestLead,
                                                                                       latestLead );

        wres.datamodel.scale.TimeScale timeScale =
                wres.datamodel.scale.TimeScale.of( java.time.Duration.ofHours( 1 ),
                                                   wres.datamodel.scale.TimeScale.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 11.94128 ),
                                                                                                OneOrTwoDoubles.of( 0.9 ),
                                                                                                Operator.GREATER_EQUAL,
                                                                                                ThresholdDataType.LEFT ) );

        Location location = Location.of( (Long) null, FEATURE_NAME, 23.45F, 56.21F, (String) null );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                             .setTimeScale( timeScale )
                                                             .setTimeWindow( timeWindow )
                                                             .setThresholds( threshold )
                                                             .setIdentifier( datasetIdentifier )
                                                             .build();

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.RELIABILITY_DIAGRAM,
                                      null );

        Map<MetricDimension, VectorOfDoubles> fakeOutputs = new EnumMap<>( MetricDimension.class );
        fakeOutputs.put( MetricDimension.FORECAST_PROBABILITY,
                         VectorOfDoubles.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) );
        fakeOutputs.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                         VectorOfDoubles.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) );
        fakeOutputs.put( MetricDimension.SAMPLE_SIZE, VectorOfDoubles.of( 5926, 371, 540, 650, 1501 ) );

        // Fake output wrapper.
        return Collections.singletonList( DiagramStatistic.of( fakeOutputs, fakeMetadata ) );
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

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0, Instant t1 )
    {
        Map<ReferenceTimeType, Instant> times = new TreeMap<>();
        times.put( ReferenceTimeType.T0, t0 );
        times.put( ReferenceTimeType.ISSUED_TIME, t1 );

        return TimeSeriesMetadata.of( times,
                                      TimeScale.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

    private Evaluation createEvaluation()
    {

        // Create a minimal representation of an evaluation, to be augmented by 
        // that statistics metadata. See #61388
        Instant startInstant = Instant.parse( "2555-12-28T00:00:00Z" );
        Instant endInstant = Instant.parse( "2555-12-28T00:03:22Z" );

        Timestamp start = Timestamp.newBuilder().setSeconds( startInstant.getEpochSecond() ).build();
        Timestamp end = Timestamp.newBuilder().setSeconds( endInstant.getEpochSecond() ).build();

        return Evaluation.newBuilder()
                         .setEvaluationStartTime( start )
                         .setEvaluationEndTime( end )
                         .setJobIdentifier( "8391007476435859400" )
                         .build();
    }

}

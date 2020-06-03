package wres.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
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
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import com.google.protobuf.InvalidProtocolBufferException;

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
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.ScaleValidationEvent;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindow;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.Statistics;

/**
 * Tests the {@link ProtobufMessageFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ProtobufMessageFactoryTest
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

    private static final wres.datamodel.time.TimeWindow TIME_WINDOW = wres.datamodel.time.TimeWindow.of( NINTH_TIME,
                                                                                                         TENTH_TIME,
                                                                                                         ELEVENTH_TIME,
                                                                                                         TWELFTH_TIME,
                                                                                                         EARLIEST_LEAD,
                                                                                                         LATEST_LEAD );

    /**
     * Scores to serialize.
     */

    private List<DoubleScoreStatistic> scores = null;

    /**
     * Duration scores to serialize.
     */

    private List<DurationScoreStatistic> durationScores = null;

    /**
     * Duration scores to serialize.
     */

    private List<PairedStatistic<Instant, Duration>> durationDiagrams = null;

    /**
     * Diagrams to serialize.
     */

    private List<DiagramStatistic> diagrams = null;

    /**
     * Box plot statistics.
     */

    private List<BoxPlotStatistics> boxplots = null;

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

        Statistics statisticsOut = ProtobufMessageFactory.parse( statistics, this.ensemblePairs );

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

        Statistics firstOut = ProtobufMessageFactory.parse( statistics, this.ensemblePairs );

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
        Statistics statisticsOut = ProtobufMessageFactory.parse( statistics,
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
        Statistics statisticsOut = ProtobufMessageFactory.parse( statistics );

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
        Statistics statisticsOut = ProtobufMessageFactory.parse( statistics );

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
                ProtobufMessageFactory.parse( ELEVENTH_TIME,
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
    public void testSendAndReceiveOneStatisticsMessage() throws Exception
    {
        // Create and start the broker, clean up on completion
        try ( EmbeddedBroker embeddedBroker = new EmbeddedBroker(); )
        {
            embeddedBroker.start();

            // Load the jndi.properties, which will be used to create a connection for a consumer
            Properties properties = new Properties();
            URL config = ProtobufMessageFactoryTest.class.getClassLoader().getResource( "jndi.properties" );
            try ( InputStream stream = config.openStream() )
            {
                properties.load( stream );
            }

            // Create a connection factory for the message consumer (producer is abstracted by StatisticsMessager)
            Context context = new InitialContext( properties );
            // Some casting, hey ho
            ConnectionFactory factory = (ConnectionFactory) context.lookup( "statisticsFactory" );
            Destination topic = (Destination) context.lookup( "statisticsTopic" );

            // Post a message and then consume it using asynchronous pub-sub style messaging
            try ( MessagerPublisher messager = MessagerPublisher.of(); // Producer
                  Connection connection = factory.createConnection(); // Consumer connection
                  Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE ); // Consumer session
                  MessageConsumer messageConsumer = session.createConsumer( topic ); ) // Consumer
            {
                // Create a statistics message
                StatisticsForProject statistics =
                        new StatisticsForProject.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                                          .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                                          .build();

                Statistics sent = ProtobufMessageFactory.parse( statistics, this.ensemblePairs );
                
                // Latch to identify when consumption is complete
                CountDownLatch consumerCount = new CountDownLatch( 1 );
                
                // Listen for messages, async
                MessageListener listener = message -> {

                    BytesMessage receivedBytes = (BytesMessage) message;

                    try
                    {
                        
                        // Create the byte array to hold the message
                        int messageLength = (int) receivedBytes.getBodyLength();

                        byte[] messageContainer = new byte[messageLength];

                        receivedBytes.readBytes( messageContainer );

                        Statistics received = Statistics.parseFrom( messageContainer );
                        
                        // Received message equals sent message
                        assertEquals( received, sent );
                        
                        consumerCount.countDown();
                    }
                    catch ( JMSException | InvalidProtocolBufferException e )
                    {
                        throw new StatisticsMessageException( "While attempting to listen for statistics messages.",
                                                              e );
                    }
                };
                
                // Start the consumer connection 
                connection.start();               

                // Subscribe the listener to the consumer
                messageConsumer.setMessageListener( listener );

                // Publish a message to the statistics topic with an arbitrary identifier and correlation identifier
                // The message identifier must begin with "ID:"
                messager.publish( ByteBuffer.wrap( sent.toByteArray() ), "ID:1234567", "89101112" );
                
                // Await the sooner of all messages read and a timeout
                boolean done = consumerCount.await( 2000L, TimeUnit.MILLISECONDS );

                if ( !done )
                {
                    fail( "Failed to consume an expected statistics message within the timeout period of 2000ms." );
                }
            }
        }
    }    
    
    /**
     * Returns a {@link List} containing several {@link DoubleScoreStatistic} for one pool.
     * 
     * @return several score statistics for one pool
     */

    private List<DoubleScoreStatistic> getScoreStatisticsForOnePool()
    {
        wres.datamodel.scale.TimeScale timeScale =
                wres.datamodel.scale.TimeScale.of( java.time.Duration.ofHours( 1 ),
                                                   wres.datamodel.scale.TimeScale.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ) );

        Location location = Location.of( (Long) null, FEATURE_NAME, 23.45F, 56.21F, (String) null );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new SampleMetadataBuilder().setMeasurementUnit( CMS )
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
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new SampleMetadataBuilder().setMeasurementUnit( CMS )
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

        Map<MetricDimension, VectorOfDoubles> fakeOutputs = new EnumMap<>( MetricDimension.class );
        fakeOutputs.put( MetricDimension.FORECAST_PROBABILITY,
                         VectorOfDoubles.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) );
        fakeOutputs.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                         VectorOfDoubles.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) );
        fakeOutputs.put( MetricDimension.SAMPLE_SIZE, VectorOfDoubles.of( 5926, 371, 540, 650, 1501 ) );

        // Fake output wrapper.
        return Collections.unmodifiableList( List.of( DiagramStatistic.of( fakeOutputs, fakeMetadata ) ) );
    }

    /**
     * Returns a {@link List} containing a {@link BoxPlotStatistics} for one pool.
     * 
     * @return the box plot statistics for one pool
     */

    private List<BoxPlotStatistics> getBoxPlotsForOnePool()
    {
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
                DatasetIdentifier.of( location, SQIN, HEFS, ESP, LeftOrRightOrBaseline.RIGHT );

        SampleMetadata metadata = new SampleMetadataBuilder().setMeasurementUnit( CMS )
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

        List<BoxPlotStatistic> fakeOutputs = new ArrayList<>();

        VectorOfDoubles probabilities = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

        BoxPlotStatistic one = BoxPlotStatistic.of( probabilities,
                                                    VectorOfDoubles.of( 1, 2, 3, 4, 5 ),
                                                    fakeMetadata,
                                                    11,
                                                    MetricDimension.OBSERVED_VALUE );

        BoxPlotStatistic two = BoxPlotStatistic.of( probabilities,
                                                    VectorOfDoubles.of( 6, 7, 8, 9, 10 ),
                                                    fakeMetadata,
                                                    22,
                                                    MetricDimension.OBSERVED_VALUE );

        BoxPlotStatistic three = BoxPlotStatistic.of( probabilities,
                                                      VectorOfDoubles.of( 11, 12, 13, 14, 15 ),
                                                      fakeMetadata,
                                                      33,
                                                      MetricDimension.OBSERVED_VALUE );


        fakeOutputs.add( one );
        fakeOutputs.add( two );
        fakeOutputs.add( three );

        StatisticMetadata fakeMetadataTwo =
                StatisticMetadata.of( metadata,
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                      null );

        List<BoxPlotStatistic> fakeOutputsTwo = new ArrayList<>();

        BoxPlotStatistic four = BoxPlotStatistic.of( probabilities,
                                                     VectorOfDoubles.of( 16, 17, 18, 19, 20 ),
                                                     fakeMetadataTwo,
                                                     73,
                                                     MetricDimension.ENSEMBLE_MEAN );

        BoxPlotStatistic five = BoxPlotStatistic.of( probabilities,
                                                     VectorOfDoubles.of( 21, 22, 23, 24, 25 ),
                                                     fakeMetadataTwo,
                                                     92,
                                                     MetricDimension.ENSEMBLE_MEAN );

        BoxPlotStatistic six = BoxPlotStatistic.of( probabilities,
                                                    VectorOfDoubles.of( 26, 27, 28, 29, 30 ),
                                                    fakeMetadataTwo,
                                                    111,
                                                    MetricDimension.ENSEMBLE_MEAN );

        fakeOutputsTwo.add( four );
        fakeOutputsTwo.add( five );
        fakeOutputsTwo.add( six );

        // Fake output wrapper.
        return List.of( BoxPlotStatistics.of( fakeOutputs, fakeMetadata ),
                        BoxPlotStatistics.of( fakeOutputsTwo, fakeMetadataTwo ) );
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

    private List<DurationScoreStatistic> getDurationScoreStatisticsForOnePool()
    {
        TimeWindow timeOne =
                TimeWindow.of( FIRST_TIME,
                               FIFTH_TIME,
                               FIRST_TIME,
                               SEVENTH_TIME,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        Map<MetricConstants, Duration> fakeOutputs = new EnumMap<>( MetricConstants.class );
        fakeOutputs.put( MetricConstants.MEAN, Duration.ofHours( 1 ) );
        fakeOutputs.put( MetricConstants.MEDIAN, Duration.ofHours( 2 ) );
        fakeOutputs.put( MetricConstants.MAXIMUM, Duration.ofHours( 3 ) );

        return Collections.singletonList( DurationScoreStatistic.of( fakeOutputs, fakeMetadata ) );
    }

    private List<PairedStatistic<Instant, Duration>> getDurationDiagramStatisticsForOnePool()
    {
        TimeWindow timeOne =
                TimeWindow.of( FIRST_TIME,
                               FIFTH_TIME,
                               FIRST_TIME,
                               SEVENTH_TIME,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        return Collections.singletonList( PairedStatistic.of( fakeOutputs, fakeMetadata ) );
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
                                      CMS.getUnit() );
    }

}

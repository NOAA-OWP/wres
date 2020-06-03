package wres.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
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
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.commons.lang3.tuple.Pair;
import org.capnproto.EnumList;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.PrimitiveList;
import org.capnproto.Serialize;
import org.capnproto.StructList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.statistics.generated.capnp.DiagramStatisticOuter;
import wres.statistics.generated.capnp.DurationOuter;
import wres.statistics.generated.capnp.DiagramStatisticOuter.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.capnp.GeometryOuter.Geometry;
import wres.statistics.generated.capnp.PairsOuter;
import wres.statistics.generated.capnp.PoolOuter.Pool;
import wres.statistics.generated.capnp.ReferenceTimeTypeOuter;
import wres.statistics.generated.capnp.ScoreMetricOuter;
import wres.statistics.generated.capnp.ScoreMetricOuter.ScoreMetric.ScoreMetricComponent;
import wres.statistics.generated.capnp.ScoreStatisticOuter;
import wres.statistics.generated.capnp.ScoreStatisticOuter.ScoreStatistic.ScoreStatisticComponent;
import wres.statistics.generated.capnp.DiagramMetricOuter;
import wres.statistics.generated.capnp.DiagramMetricOuter.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.capnp.StatisticsOuter.Statistics;
import wres.statistics.generated.capnp.ThresholdOuter;
import wres.statistics.generated.capnp.TimeWindowOuter;
import wres.statistics.generated.capnp.TimestampOuter;

/**
 * Tests the {@link CapnprotoMessageFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

@Ignore
public class CapnprotoMessageFactoryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CapnprotoMessageFactoryTest.class );

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
     * Diagrams to serialize.
     */

    private List<DiagramStatistic> diagrams = null;

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
        this.diagrams = this.getReliabilityDiagramForOnePool();
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

        MessageBuilder message = new MessageBuilder();

        // Add the statistics
        Statistics.Builder builder = CapnprotoMessageFactory.parse( message, statistics );

        // Add the pairs
        CapnprotoMessageFactory.parseEnsemblePairs( builder.getPool().getPairs(), this.ensemblePairs );

        Path path = this.outputDirectory.resolve( "statistics.capnp" );

        try ( WritableByteChannel outputChannel =
                Files.newByteChannel( path,
                                      StandardOpenOption.WRITE,
                                      StandardOpenOption.CREATE,
                                      StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            Serialize.write( outputChannel, message );
        }

        assertTrue( path.toFile().exists() );
    }

    // It is ESSENTIAL that logging is suppressed when un-ignoring this method because the broker is extremely chatty
    // and this test may iterate millions of messages. Logging time will vastly exceed messaging time.
    @Test
    @Ignore
    public void testSendAndReceiveNStatisticsMessages() throws Exception
    {
        // Create and start the broker, clean up on completion
        try ( EmbeddedBroker embeddedBroker = new EmbeddedBroker(); )
        {
            embeddedBroker.start();

            // Load the jndi.properties, which will be used to create a connection for a message consumer
            Properties properties = new Properties();
            URL config = CapnprotoMessageFactoryTest.class.getClassLoader().getResource( "jndi.properties" );
            try ( InputStream stream = config.openStream() )
            {
                properties.load( stream );
            }

            // Create a connection factory for the message consumer (producer is abstracted by StatisticsMessager)
            Context context = new InitialContext( properties );
            String factoryName = "statisticsFactory";
            String destinationName = "statisticsQueue"; //p2p queue, rather than a pub/sub topic

            // Some casting, hey ho
            ConnectionFactory factory = (ConnectionFactory) context.lookup( factoryName );
            Queue queue = (Queue) context.lookup( destinationName );

            // Post a message and then consume it using a blocking/p2p pattern
            try ( MessagerPublisher producer = MessagerPublisher.of( DeliveryMode.NON_PERSISTENT, // OK, for now
                                                                     Message.DEFAULT_PRIORITY,
                                                                     Message.DEFAULT_TIME_TO_LIVE, // Don't die
                                                                     destinationName,
                                                                     factoryName );
                  Connection connection = factory.createConnection(); // Consumer connection
                  Session session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE ); // Consumer session
                  MessageConsumer messageConsumer = session.createConsumer( queue ); ) // Consumer
            {

                // Create a statistics message, not instrumenting yet
                StatisticsForProject statistics =
                        new StatisticsForProject.Builder().addDoubleScoreStatistics( CompletableFuture.completedFuture( this.scores ) )
                                                          .addDiagramStatistics( CompletableFuture.completedFuture( this.diagrams ) )
                                                          .build();

                // Create a benchmark flatbuffer against which to assert equality (of hash)
                // Needs a builder. Instantiate with a default buffer of 1 KiB, grow as needed
                MessageBuilder message = new MessageBuilder();

                // Add the statistics
                Statistics.Builder builder = CapnprotoMessageFactory.parse( message, statistics );

                // Add the pairs
                CapnprotoMessageFactory.parseEnsemblePairs( builder.getPool().getPairs(), this.ensemblePairs );

                Statistics.Reader reader = builder.asReader();

                int messageHashForEquals = CapnprotoMessageFactoryTest.hashMessage( reader );

                // Stand up some counters for instrumenting
                Duration creating = Duration.ZERO;
                Duration publishing = Duration.ZERO;
                Duration receiving = Duration.ZERO;
                Duration reading = Duration.ZERO;
                Duration consuming = Duration.ZERO;
                int totalMessagesProcessed = 0;

                // Total number of messages to send and receive
                int totalMessages = 10_000_000;
                // Increment on which to report (set this to totalMessages or larger to skip all messaging)
                int reportIncrement = 500_000;
                // CountDownLatch to identify when consumption is complete
                CountDownLatch consumerCount = new CountDownLatch( totalMessages );

                // Start the consumer connection 
                connection.start();

                // Iterate the messages, p2p: create; publish; receive; read into abstraction; access/consume.
                for ( int i = 1; i <= totalMessages; i++ )
                {
                    // 1. Create
                    Instant one = Instant.now();
                    MessageBuilder next = new MessageBuilder();
                    Statistics.Builder nextStats = CapnprotoMessageFactory.parse( next, statistics );

                    // Add the pairs
                    CapnprotoMessageFactory.parseEnsemblePairs( nextStats.getPool().getPairs(),
                                                                this.ensemblePairs );

                    // 2. Publish
                    Instant two = Instant.now();

                    ByteBuffer buffer =
                            CapnprotoMessageFactory.getByteBufferFromOutputSegments( next );

                    producer.publish( buffer, "ID:" + i, "89101112" );

                    Instant three = Instant.now();

                    creating = creating.plus( Duration.between( one, two ) );
                    publishing = publishing.plus( Duration.between( two, three ) );

                    // Report on sending (1 and 2)
                    if ( i % reportIncrement == 0 )
                    {
                        LOGGER.info( "{} messages sent.", i );
                    }

                    // 3. Receive
                    Instant oneA = Instant.now();
                    BytesMessage receivedBytes = (BytesMessage) messageConsumer.receive();
                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();
                    byte[] messageContainer = new byte[messageLength];
                    receivedBytes.readBytes( messageContainer );

                    // 4. Read into abstraction
                    Instant twoA = Instant.now();
                    MessageReader in = Serialize.read( ByteBuffer.wrap( messageContainer ) );

                    Statistics.Reader received = in.getRoot( Statistics.factory );
                    Instant threeA = Instant.now();

                    // 5. Access/consume, simulated by finding a hash and asserting equality against expected
                    int messageHash = CapnprotoMessageFactoryTest.hashMessage( received );
                    assertEquals( messageHash, messageHashForEquals );

                    Instant fourA = Instant.now();

                    // Instrument receive/read
                    receiving = receiving.plus( Duration.between( oneA, twoA ) );
                    reading = reading.plus( Duration.between( twoA, threeA ) );
                    consuming = consuming.plus( Duration.between( threeA, fourA ) );

                    // Another one bytes the dust (sorry)
                    totalMessagesProcessed++;
                    consumerCount.countDown();

                    // Report on receiving (3, 4 and 5)
                    if ( totalMessagesProcessed % reportIncrement == 0 )
                    {
                        LOGGER.info( "{} messages received.", totalMessagesProcessed );
                    }
                }


                // Await the sooner of all messages read and a timeout
                boolean done = consumerCount.await( 500000L, TimeUnit.MILLISECONDS );

                if ( !done )
                {
                    fail( "Failed to consume an expected statistics message within the timeout period of 2000ms." );
                }

                // Print details of time taken
                Duration totalTime = creating.plus( publishing )
                                             .plus( receiving )
                                             .plus( reading )
                                             .plus( consuming );

                Duration totalTimeSending = creating.plus( publishing );
                Duration totalTimeReceiving = receiving.plus( reading ).plus( consuming );

                double receivingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) totalTimeReceiving.toMillis()
                                                           / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );
                double sendingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) totalTimeSending.toMillis() / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );
                double creatingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) creating.toMillis() / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );
                double publishingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) publishing.toMillis() / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );
                double readingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) receiving.toMillis() / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );
                double parsingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) reading.toMillis() / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );
                double assertingPercent =
                        CapnprotoMessageFactoryTest.round()
                                                   .apply( (double) consuming.toMillis()
                                                           / totalTime.toMillis()
                                                           * 100.0,
                                                           1 );

                // Summarize
                StringJoiner joiner = new StringJoiner( System.getProperty( "line.separator" ) );


                joiner.add( Instant.now() + ":" );
                joiner.add( "    Total time instrumented when sending " + totalMessages
                            + " messages and receiving "
                            + totalMessagesProcessed
                            + " messages: "
                            + totalTime
                            + ". Of which:" );
                joiner.add( "        Total time spent sending messages: " + totalTimeSending
                            + " ("
                            + sendingPercent
                            + "%). Of which:" );
                joiner.add( "            Time spent creating statistics messages from wres.datamodel."
                            + "statistics.StatisticsForProject: "
                            + creating
                            + " ("
                            + creatingPercent
                            + "%)." );
                joiner.add( "            Time spent publishing statistics messages to statistics topic: "
                            + publishing
                            + " ("
                            + publishingPercent
                            + "%)." );
                joiner.add( "        Total time spent receiving messages: " + totalTimeReceiving
                            + " ("
                            + receivingPercent
                            + "%). Of which:" );
                joiner.add( "            Time spent reading statistics message bytes from topic: "
                            + receiving
                            + " ("
                            + readingPercent
                            + "%) " );
                joiner.add( "            Time spent creating statistics messages from bytes: "
                            + reading
                            + " ("
                            + parsingPercent
                            + "%)." );
                joiner.add( "            Time spent hashing received message and comparing it to hash of sent "
                            + "message: "
                            + consuming
                            + " ("
                            + assertingPercent
                            + "%)." );

                String logString = joiner.toString();
                LOGGER.info( logString );
            }
        }
    }

    /**
     * Hashes diagrams, ordinary scores and the pool only (without baseline pairs). This method is largely unchecked. 
     * Purely to simulate consumption and avoid the compiler optimizing consumption away. Beware when comparing this 
     * implementation across formats, especially if calling multiple times per instance (protobuf memoizes hashes, 
     * whereas this is a static helper that obviously does not).
     * 
     * @param message the message
     * @return a hash
     */

    private static int hashMessage( Statistics.Reader message )
    {
        int hash = 19;

        hash = (hash * 31) + CapnprotoMessageFactoryTest.hashDiagrams( message.getDiagrams() );
        hash = (hash * 31 ) + CapnprotoMessageFactoryTest.hashScores( message.getScores() );

        // Pool
        Pool.Reader pool = message.getPool();

        StructList.Reader<Geometry.Reader> geoms = pool.getGeometries();

        int geomLength = geoms.size();

        for ( int i = 0; i < geomLength; i++ )
        {
            Geometry.Reader geom = geoms.get( i );

            hash = ( hash * 31 ) + Double.hashCode( geom.getLatitude() );
            hash = ( hash * 31 ) + Double.hashCode( geom.getLongitude() );

            hash = ( hash * 31 ) + geom.getName().toString().hashCode();
        }

        TimeWindowOuter.TimeWindow.Reader window = pool.getTimeWindow();

        DurationOuter.Duration.Reader early = window.getEarliestLeadDuration();

        hash = ( hash * 31 ) + Long.hashCode( early.getSeconds() );
        hash = ( hash * 31 ) + Integer.hashCode( early.getNanos() );

        DurationOuter.Duration.Reader late = window.getLatestLeadDuration();

        hash = ( hash * 31 ) + Long.hashCode( late.getSeconds() );
        hash = ( hash * 31 ) + Integer.hashCode( late.getNanos() );

        TimestampOuter.Timestamp.Reader earlyRef = window.getEarliestReferenceTime();

        hash = ( hash * 31 ) + Long.hashCode( earlyRef.getSeconds() );
        hash = ( hash * 31 ) + Integer.hashCode( earlyRef.getNanos() );

        TimestampOuter.Timestamp.Reader lateRef = window.getLatestReferenceTime();

        hash = ( hash * 31 ) + Long.hashCode( lateRef.getSeconds() );
        hash = ( hash * 31 ) + Integer.hashCode( lateRef.getNanos() );

        TimestampOuter.Timestamp.Reader earlyVal = window.getEarliestValidTime();

        hash = ( hash * 31 ) + Long.hashCode( earlyVal.getSeconds() );
        hash = ( hash * 31 ) + Integer.hashCode( earlyVal.getNanos() );

        TimestampOuter.Timestamp.Reader lateVal = window.getLatestValidTime();

        hash = ( hash * 31 ) + Long.hashCode( lateVal.getSeconds() );
        hash = ( hash * 31 ) + Integer.hashCode( lateVal.getNanos() );

        EnumList.Reader<ReferenceTimeTypeOuter.ReferenceTimeType> refTypes = window.getReferenceTimeType();

        int typesLength = refTypes.size();

        for ( int i = 0; i < typesLength; i++ )
        {
            hash = ( hash * 31 ) + Objects.hashCode( refTypes.get( i ) );
        }

        PairsOuter.Pairs.Reader pairs = pool.getPairs();
        StructList.Reader<PairsOuter.Pairs.TimeSeriesOfPairs.Reader> series = pairs.getTimeSeries();

        int seriesLength = series.size();

        for ( int i = 0; i < seriesLength; i++ )
        {
            PairsOuter.Pairs.TimeSeriesOfPairs.Reader s = series.get( i );
            StructList.Reader<PairsOuter.Pairs.Pair.Reader> p = s.getPairs();

            int pairsLength = p.size();

            for ( int j = 0; j < pairsLength; j++ )
            {
                PairsOuter.Pairs.Pair.Reader pair = p.get( j );

                PrimitiveList.Double.Reader left = pair.getLeft();

                int leftLength = left.size();

                for ( int k = 0; k < leftLength; k++ )
                {
                    hash = ( hash * 31 ) + Double.hashCode( left.get( k ) );
                }

                PrimitiveList.Double.Reader right = pair.getRight();

                int rightLength = right.size();

                for ( int k = 0; k < rightLength; k++ )
                {
                    hash = ( hash * 31 ) + Double.hashCode( right.get( k ) );
                }
            }
        }

        return hash;
    }
    
    /**
     * Hashes diagrams.
     * 
     * @param diagrams the diagrams to hash
     * @return a hash
     */

    private static int hashDiagrams( StructList.Reader<DiagramStatisticOuter.DiagramStatistic.Reader> diagrams )
    {
        int diagLength = diagrams.size();

        int hash = 19;

        for ( int i = 0; i < diagLength; i++ )
        {
            DiagramStatisticOuter.DiagramStatistic.Reader statistic = diagrams.get( i );
            ThresholdOuter.Threshold.Reader thresholdOne = statistic.getEventThreshold();

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getLeftThresholdValue() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getLeftThresholdProbability() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getRightThresholdProbability() );

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getRightThresholdValue() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.getName().toString() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.getType() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.getThresholdValueUnits().toString() );

            ThresholdOuter.Threshold.Reader thresholdTwo = statistic.getDecisionThreshold();

            if ( Objects.nonNull( thresholdTwo ) )
            {
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getLeftThresholdValue() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getLeftThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getRightThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getRightThresholdValue() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.getName().toString() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.getType() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.getThresholdValueUnits().toString() );
            }

            DiagramMetricOuter.DiagramMetric.Reader metric = statistic.getMetric();

            hash = ( hash * 31 ) + Objects.hashCode( metric.getName() );

            StructList.Reader<DiagramMetricComponent.Reader> components = metric.getComponents();

            int compLength = components.size();

            for ( int j = 0; j < compLength; j++ )
            {
                DiagramMetricComponent.Reader comp = components.get( j );

                hash = ( hash * 31 ) + Double.hashCode( comp.getMinimum() );
                hash = ( hash * 31 ) + Double.hashCode( comp.getMaximum() );
                hash = ( hash * 31 ) + Objects.hashCode( comp.getName() );
                hash = ( hash * 31 ) + comp.getUnits().toString().hashCode();
            }

            StructList.Reader<DiagramStatisticComponent.Reader> diagramComponents = statistic.getStatistics();

            int stLength = diagramComponents.size();

            for ( int j = 0; j < stLength; j++ )
            {
                DiagramStatisticComponent.Reader comp = diagramComponents.get( j );

                hash = ( hash * 31 ) + Objects.hashCode( comp.getName().toString() );

                PrimitiveList.Double.Reader values = comp.getValues();

                int length = values.size();

                for ( int k = 0; k < length; k++ )
                {
                    hash = ( hash * 31 ) + Double.hashCode( values.get( k ) );
                }
            }
        }

        return hash;
    }    
    
    /**
     * Hashes scores.
     * 
     * @param scores the scores to hash
     * @return a hash
     */

    private static int hashScores( StructList.Reader<ScoreStatisticOuter.ScoreStatistic.Reader> scores )
    {
        int hash = 19;

        int scoresLength = scores.size();

        for ( int i = 0; i < scoresLength; i++ )
        {
            ScoreStatisticOuter.ScoreStatistic.Reader score = scores.get( i );

            ThresholdOuter.Threshold.Reader thresholdOne = score.getEventThreshold();

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getLeftThresholdValue() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getLeftThresholdProbability() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getRightThresholdProbability() );

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.getRightThresholdValue() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.getName().toString() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.getType() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.getThresholdValueUnits().toString() );

            ThresholdOuter.Threshold.Reader thresholdTwo = score.getDecisionThreshold();

            if ( Objects.nonNull( thresholdTwo ) )
            {
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getLeftThresholdValue() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getLeftThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getRightThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.getRightThresholdValue() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.getName().toString() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.getType() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.getThresholdValueUnits().toString() );
            }

            ScoreMetricOuter.ScoreMetric.Reader metric = score.getMetric();

            hash = ( hash * 31 ) + Objects.hashCode( metric.getName() );

            StructList.Reader<ScoreMetricComponent.Reader> components = metric.getComponents();

            int compLength = components.size();

            for ( int j = 0; j < compLength; j++ )
            {
                ScoreMetricComponent.Reader comp = components.get( j );

                hash = ( hash * 31 ) + Double.hashCode( comp.getMinimum() );
                hash = ( hash * 31 ) + Double.hashCode( comp.getMaximum() );
                hash = ( hash * 31 ) + Double.hashCode( comp.getOptimum() );
                hash = ( hash * 31 ) + Objects.hashCode( comp.getName().toString() );
                hash = ( hash * 31 ) + comp.getUnits().toString().hashCode();
            }

            StructList.Reader<ScoreStatisticComponent.Reader> scoreComponents = score.getStatistics();

            int stLength = scoreComponents.size();

            for ( int j = 0; j < stLength; j++ )
            {
                ScoreStatisticComponent.Reader comp = scoreComponents.get( j );

                hash = ( hash * 31 ) + Objects.hashCode( comp.getName() );

                hash = ( hash * 31 ) + Double.hashCode( comp.getValue() );
            }

        }

        return hash;
    }       

    /**
     * @return a function that rounds to a prescribed number of d.p.
     */

    private static BiFunction<Double, Integer, Double> round()
    {
        return ( input, digits ) -> {

            if ( Double.isFinite( input ) )
            {
                BigDecimal bd = new BigDecimal( Double.toString( input ) );
                bd = bd.setScale( digits, RoundingMode.HALF_UP );

                return bd.doubleValue();
            }

            return input;
        };
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
                                      CMS.getUnit() );
    }

}

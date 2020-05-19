package wres.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
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
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.IntVector;

import wres.statistics.generated.flatbuffers.Statistics;

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

import javax.jms.Message;

/**
 * Tests the {@link ProtobufMessageFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class FlatbuffersMessageFactoryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FlatbuffersMessageFactoryTest.class );

    private static final String ESP = "ESP";
    private static final String HEFS = "HEFS";
    private static final String SQIN = "SQIN";
    private static final String VARIABLE_NAME = "Streamflow";
    private static final String FEATURE_NAME = "DRRC2";
    private static final MeasurementUnit CMS = MeasurementUnit.of( "CMS" );

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

        FlatBufferBuilder builder = new FlatBufferBuilder();

        int stats = FlatbuffersMessageFactory.parse( builder, statistics, this.ensemblePairs );

        builder.finish( stats );

        byte[] bytesOut = builder.sizedByteArray();

        Path path = this.outputDirectory.resolve( "statistics.fb" );

        try ( OutputStream stream =
                Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            stream.write( builder.sizedByteArray() );
        }

        byte[] bytesIn = null;
        try ( InputStream stream =
                Files.newInputStream( path ) )
        {
            bytesIn = stream.readAllBytes();
        }

        // Superficial assertion for now because flatbuffers do not override equals
        assertTrue( Arrays.equals( bytesIn, bytesOut ) );

        // Delete if succeeded
        Files.deleteIfExists( path );
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
            URL config = FlatbuffersMessageFactoryTest.class.getClassLoader().getResource( "jndi.properties" );
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
            try ( StatisticsMessager producer = StatisticsMessager.of( DeliveryMode.NON_PERSISTENT, // OK, for now
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
                FlatBufferBuilder builder = new FlatBufferBuilder();
                int stats = FlatbuffersMessageFactory.parse( builder, statistics, this.ensemblePairs );
                builder.finish( stats );
                Statistics sentForEquals = Statistics.getRootAsStatistics( builder.dataBuffer() );
                int messageHashForEquals = FlatbuffersMessageFactoryTest.hashMessage( sentForEquals );

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
                    FlatBufferBuilder next = new FlatBufferBuilder();
                    int sent = FlatbuffersMessageFactory.parse( next, statistics, this.ensemblePairs );
                    next.finish( sent );

                    // 2. Publish
                    Instant two = Instant.now();
                    producer.publish( next.sizedByteArray(), "ID:" + i, "89101112" );
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
                    Statistics received = Statistics.getRootAsStatistics( ByteBuffer.wrap( messageContainer ) );
                    Instant threeA = Instant.now();

                    // 5. Access/consume, simulated by finding a hash and asserting equality against expected
                    int messageHash = FlatbuffersMessageFactoryTest.hashMessage( received );
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
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) totalTimeReceiving.toMillis()
                                                             / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );
                double sendingPercent =
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) totalTimeSending.toMillis() / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );
                double creatingPercent =
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) creating.toMillis() / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );
                double publishingPercent =
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) publishing.toMillis() / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );
                double readingPercent =
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) receiving.toMillis() / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );
                double parsingPercent =
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) reading.toMillis() / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );
                double assertingPercent =
                        FlatbuffersMessageFactoryTest.round()
                                                     .apply( (double) consuming.toMillis()
                                                             / totalTime.toMillis()
                                                             * 100.0,
                                                             1 );

                // Summarize to standard out
                System.out.println( Instant.now() + ":" );
                System.out.println( "    Total time instrumented when sending " + totalMessages
                                    + " messages and receiving "
                                    + totalMessagesProcessed
                                    + " messages: "
                                    + totalTime
                                    + ". Of which:" );
                System.out.println( "        Total time spent sending messages: " + totalTimeSending
                                    + " ("
                                    + sendingPercent
                                    + "%). Of which:" );
                System.out.println( "            Time spent creating statistics messages from wres.datamodel."
                                    + "statistics.StatisticsForProject: "
                                    + creating
                                    + " ("
                                    + creatingPercent
                                    + "%)." );
                System.out.println( "            Time spent publishing statistics messages to statistics topic: "
                                    + publishing
                                    + " ("
                                    + publishingPercent
                                    + "%)." );
                System.out.println( "        Total time spent receiving messages: " + totalTimeReceiving
                                    + " ("
                                    + receivingPercent
                                    + "%). Of which:" );
                System.out.println( "            Time spent reading statistics message bytes from topic: "
                                    + receiving
                                    + " ("
                                    + readingPercent
                                    + "%) " );
                System.out.println( "            Time spent creating statistics messages from bytes: "
                                    + reading
                                    + " ("
                                    + parsingPercent
                                    + "%)." );
                System.out.println( "            Time spent hashing received message and comparing it to hash of sent "
                                    + "message: "
                                    + consuming
                                    + " ("
                                    + assertingPercent
                                    + "%)." );
            }
        }
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
     * Hashes diagrams, ordinary scores and the pool only (without baseline pairs). This method is largely unchecked. 
     * Purely to simulate consumption and avoid the compiler optimizing consumption away. Beware when comparing this 
     * implementation across formats, especially if calling multiple times per instance (protobuf memoizes hashes, 
     * whereas this is a static helper that obviously does not).
     * 
     * @param message the message
     * @return a hash
     */

    private static int hashMessage( wres.statistics.generated.flatbuffers.Statistics message )
    {
        wres.statistics.generated.flatbuffers.DiagramStatistic.Vector diagrams = message.diagramsVector();

        int diagLength = message.diagramsLength();

        int hash = 19;

        for ( int i = 0; i < diagLength; i++ )
        {
            wres.statistics.generated.flatbuffers.DiagramStatistic statistic = diagrams.get( i );
            wres.statistics.generated.flatbuffers.Threshold thresholdOne = statistic.eventThreshold();

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.leftThresholdValue() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.leftThresholdProbability() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.rightThresholdProbability() );

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.rightThresholdValue() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.name() );
            hash = ( hash * 31 ) + Integer.hashCode( thresholdOne.type() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.thresholdValueUnits() );

            wres.statistics.generated.flatbuffers.Threshold thresholdTwo = statistic.decisionThreshold();

            if ( Objects.nonNull( thresholdTwo ) )
            {
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.rightThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.leftThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.leftThresholdValue() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.rightThresholdValue() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.name().hashCode() );
                hash = ( hash * 31 ) + Integer.hashCode( thresholdTwo.type() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.thresholdValueUnits() );
            }

            wres.statistics.generated.flatbuffers.DiagramMetric metric = statistic.metric();

            hash = ( hash * 31 ) + Integer.hashCode( metric.name() );

            wres.statistics.generated.flatbuffers.DiagramMetric_.DiagramMetricComponent.Vector components =
                    metric.componentsVector();

            int compLength = components.length();

            for ( int j = 0; j < compLength; j++ )
            {
                wres.statistics.generated.flatbuffers.DiagramMetric_.DiagramMetricComponent comp = components.get( j );

                hash = ( hash * 31 ) + Double.hashCode( comp.minimum() );
                hash = ( hash * 31 ) + Double.hashCode( comp.maximum() );
                hash = ( hash * 31 ) + Integer.hashCode( comp.name() );
                hash = ( hash * 31 ) + comp.units().hashCode();
            }

            wres.statistics.generated.flatbuffers.DiagramStatistic_.DiagramStatisticComponent.Vector statsComp =
                    statistic.statisticsVector();

            int stLength = statsComp.length();

            for ( int j = 0; j < stLength; j++ )
            {
                wres.statistics.generated.flatbuffers.DiagramStatistic_.DiagramStatisticComponent comp =
                        statsComp.get( j );

                hash = ( hash * 31 ) + Integer.hashCode( comp.name() );

                DoubleVector values = comp.valuesVector();

                int length = values.length();

                for ( int k = 0; k < length; k++ )
                {
                    hash = ( hash * 31 ) + Double.hashCode( values.get( k ) );
                }
            }
        }

        // Scores
        wres.statistics.generated.flatbuffers.ScoreStatistic.Vector scores = message.scoresVector();

        int scoresLength = message.scoresLength();

        for ( int i = 0; i < scoresLength; i++ )
        {
            wres.statistics.generated.flatbuffers.ScoreStatistic score = scores.get( i );

            wres.statistics.generated.flatbuffers.Threshold thresholdOne = score.eventThreshold();

            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.rightThresholdProbability() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.leftThresholdProbability() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.leftThresholdValue() );
            hash = ( hash * 31 ) + Double.hashCode( thresholdOne.rightThresholdValue() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.name() );
            hash = ( hash * 31 ) + Integer.hashCode( thresholdOne.type() );
            hash = ( hash * 31 ) + Objects.hashCode( thresholdOne.thresholdValueUnits() );

            wres.statistics.generated.flatbuffers.Threshold thresholdTwo = score.decisionThreshold();

            if ( Objects.nonNull( thresholdTwo ) )
            {
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.rightThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.leftThresholdProbability() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.leftThresholdValue() );
                hash = ( hash * 31 ) + Double.hashCode( thresholdTwo.rightThresholdValue() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.name() );
                hash = ( hash * 31 ) + Integer.hashCode( thresholdTwo.type() );
                hash = ( hash * 31 ) + Objects.hashCode( thresholdTwo.thresholdValueUnits() );
            }
            wres.statistics.generated.flatbuffers.ScoreMetric metric = score.metric();

            hash = ( hash * 31 ) + Integer.hashCode( metric.name() );

            wres.statistics.generated.flatbuffers.ScoreMetric_.ScoreMetricComponent.Vector components =
                    metric.componentsVector();

            int compLength = components.length();

            for ( int j = 0; j < compLength; j++ )
            {
                wres.statistics.generated.flatbuffers.ScoreMetric_.ScoreMetricComponent comp = components.get( j );

                hash = ( hash * 31 ) + Double.hashCode( comp.minimum() );
                hash = ( hash * 31 ) + Double.hashCode( comp.maximum() );
                hash = ( hash * 31 ) + Double.hashCode( comp.optimum() );
                hash = ( hash * 31 ) + Integer.hashCode( comp.name() );
                hash = ( hash * 31 ) + comp.units().hashCode();
            }

            wres.statistics.generated.flatbuffers.ScoreStatistic_.ScoreStatisticComponent.Vector statsComp =
                    score.statisticsVector();

            int stLength = statsComp.length();

            for ( int j = 0; j < stLength; j++ )
            {
                wres.statistics.generated.flatbuffers.ScoreStatistic_.ScoreStatisticComponent comp = statsComp.get( j );

                hash = ( hash * 31 ) + Integer.hashCode( comp.name() );

                hash = ( hash * 31 ) + Double.hashCode( comp.value() );
            }

        }

        // Pool
        wres.statistics.generated.flatbuffers.Pool pool = message.pool();

        wres.statistics.generated.flatbuffers.Geometry.Vector geoms = pool.geometriesVector();

        int geomLength = geoms.length();

        for ( int i = 0; i < geomLength; i++ )
        {
            wres.statistics.generated.flatbuffers.Geometry geom = geoms.get( i );

            hash = ( hash * 31 ) + Double.hashCode( geom.latitude() );
            hash = ( hash * 31 ) + Double.hashCode( geom.longitude() );

            hash = ( hash * 31 ) + geom.name().hashCode();
        }

        wres.statistics.generated.flatbuffers.TimeWindow window = pool.timeWindow();

        wres.statistics.generated.flatbuffers.Duration early = window.earliestLeadDuration();

        hash = ( hash * 31 ) + Long.hashCode( early.seconds() );
        hash = ( hash * 31 ) + Integer.hashCode( early.nanos() );

        wres.statistics.generated.flatbuffers.Duration late = window.latestLeadDuration();

        hash = ( hash * 31 ) + Long.hashCode( late.seconds() );
        hash = ( hash * 31 ) + Integer.hashCode( late.nanos() );

        wres.statistics.generated.flatbuffers.Timestamp earlyRef = window.earliestReferenceTime();

        hash = ( hash * 31 ) + Long.hashCode( earlyRef.seconds() );
        hash = ( hash * 31 ) + Integer.hashCode( earlyRef.nanos() );

        wres.statistics.generated.flatbuffers.Timestamp lateRef = window.latestReferenceTime();

        hash = ( hash * 31 ) + Long.hashCode( lateRef.seconds() );
        hash = ( hash * 31 ) + Integer.hashCode( lateRef.nanos() );

        wres.statistics.generated.flatbuffers.Timestamp earlyVal = window.earliestValidTime();

        hash = ( hash * 31 ) + Long.hashCode( earlyVal.seconds() );
        hash = ( hash * 31 ) + Integer.hashCode( earlyVal.nanos() );

        wres.statistics.generated.flatbuffers.Timestamp lateVal = window.latestValidTime();

        hash = ( hash * 31 ) + Long.hashCode( lateVal.seconds() );
        hash = ( hash * 31 ) + Integer.hashCode( lateVal.nanos() );

        IntVector refTypes = window.referenceTimeTypeVector();

        int typesLength = refTypes.length();

        for ( int i = 0; i < typesLength; i++ )
        {
            hash = ( hash * 31 ) + Integer.hashCode( refTypes.get( i ) );
        }

        wres.statistics.generated.flatbuffers.Pairs pairs = pool.pairs();
        wres.statistics.generated.flatbuffers.Pairs_.TimeSeriesOfPairs.Vector series = pairs.timeSeriesVector();

        int seriesLength = series.length();

        for ( int i = 0; i < seriesLength; i++ )
        {
            wres.statistics.generated.flatbuffers.Pairs_.TimeSeriesOfPairs s = series.get( i );
            wres.statistics.generated.flatbuffers.Pairs_.Pair.Vector p = s.pairsVector();

            int pairsLength = p.length();

            for ( int j = 0; j < pairsLength; j++ )
            {
                wres.statistics.generated.flatbuffers.Pairs_.Pair pair = p.get( j );

                DoubleVector left = pair.leftVector();

                int leftLength = left.length();

                for ( int k = 0; k < leftLength; k++ )
                {
                    hash = ( hash * 31 ) + Double.hashCode( left.get( k ) );
                }

                DoubleVector right = pair.rightVector();

                int rightLength = right.length();

                for ( int k = 0; k < rightLength; k++ )
                {
                    hash = ( hash * 31 ) + Double.hashCode( right.get( k ) );
                }
            }
        }

        return hash;
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
                                      CMS.getUnit() );
    }

}

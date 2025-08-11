package wres.writing.client;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.components.ThresholdOperator;
import wres.config.components.ThresholdOrientation;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.EvaluationEventUtilities;
import wres.events.EvaluationMessager;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.client.MessagingClient;
import wres.events.subscribe.ConsumerFactory;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link WritingConsumerFactory} in the context of a {@link MessagingClient}.
 *
 * @author James Brown
 */

class WritingConsumerFactoryTest
{
    /**
     * Embedded broker.
     */

    private static EmbeddedBroker broker = null;

    /**
     * Broker connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    /**
     * A re-used string.
     */

    private static final String CMS = "CMS";

    /**
     * One evaluation for testing.
     */

    private wres.statistics.generated.Evaluation oneEvaluation;

    /**
     * Collection of statistics for testing.
     */

    private Statistics oneStatistics;

    /**
     * Path to write.
     */

    private Path outputPath;

    @BeforeAll
    static void runBeforeAllTests()
    {
        // Create and start and embedded broker
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        WritingConsumerFactoryTest.broker = EmbeddedBroker.of( properties, true );
        WritingConsumerFactoryTest.broker.start();

        // Create a connection factory to supply broker connections
        WritingConsumerFactoryTest.connections = BrokerConnectionFactory.of( properties, 2 );
    }

    @BeforeEach
    void runBeforeEachTest()
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setCsv2( Outputs.Csv2Format.getDefaultInstance() )
                                 .build();

        this.oneEvaluation = wres.statistics.generated.Evaluation.newBuilder()
                                                                 .setLeftVariableName( "QINE" )
                                                                 .setRightVariableName( "SQIN" )
                                                                 .setRightDataName( "HEFS" )
                                                                 .setBaselineDataName( "ESP" )
                                                                 .setMeasurementUnit( CMS )
                                                                 .setOutputs( outputs )
                                                                 .build();

        this.outputPath = Paths.get( System.getProperty( "java.io.tmpdir" ) );
        this.oneStatistics = this.getScoreStatisticsForOnePool();
    }

    @Test
    void publishAndConsumeOneEvaluationWithAnExternalwritingSubscriber() throws IOException
    {
        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        Set<Path> actualPathsWritten;

        // Open an evaluation, closing on completion
        Path basePath;

        // Client identifier = identifier of the one subscriber it composes
        String subscriberId = EvaluationEventUtilities.getId();

        // A factory that creates consumers on demand
        ConsumerFactory consumerFactory = new WritingConsumerFactory( subscriberId );
        MessagingClient writing = MessagingClient.of( WritingConsumerFactoryTest.connections, consumerFactory );

        // Start the writing client
        writing.start();

        try ( // This is the evaluation instance that declares png output
              EvaluationMessager evaluation = EvaluationMessager.of( this.oneEvaluation,
                                                                     WritingConsumerFactoryTest.connections,
                                                                     "aClient" ) )
        {
            // Start the evaluation
            evaluation.start();

            // Publish the statistics to a "feature" group
            evaluation.publish( this.oneStatistics, "DRRC2" );

            // Mark publication complete, which implicitly marks all groups complete
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();

            // Record the paths written to assert against
            actualPathsWritten = evaluation.getPathsWrittenBySubscribers();

            basePath = this.outputPath.resolve( "wres_evaluation_" + evaluation.getEvaluationId() );

        }
        finally
        {
            writing.stop();
        }

        // Make assertions about the writing written by the single external subscription.
        Set<Path> expectedPaths = Set.of( basePath.resolve( "evaluation.csv.gz" ),
                                          basePath.resolve( "evaluation.csvt" ) );

        Assertions.assertEquals( expectedPaths, actualPathsWritten );

        // Clean up by deleting the paths written
        for ( Path next : actualPathsWritten )
        {
            boolean ignored = next.toFile() // NOSONAR
                                  .delete();
        }

        boolean ignored = basePath.toFile() // NOSONAR
                                  .delete();
    }

    /**
     * @return several score statistics for one pool
     */

    private Statistics getScoreStatisticsForOnePool()
    {
        wres.datamodel.scale.TimeScaleOuter timeScale =
                wres.datamodel.scale.TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                        TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                                    ThresholdOperator.GREATER,
                                                                                    ThresholdOrientation.OBSERVED ) );

        Geometry geometry = MessageUtilities.getGeometry( "DRRC2", null, null, "POINT (23.45 56.21)" );

        Instant earliestValid = Instant.parse( "2551-03-20T01:00:00Z" );
        Instant latestValid = Instant.parse( "2551-03-20T12:00:00Z" );
        Instant earliestReference = Instant.parse( "2551-03-19T00:00:00Z" );
        Instant latestReference = Instant.parse( "2551-03-19T12:00:00Z" );
        Duration earliestLead = Duration.ofHours( 1 );
        Duration latestLead = Duration.ofHours( 7 );

        TimeWindow inner = MessageUtilities.getTimeWindow( earliestReference,
                                                           latestReference,
                                                           earliestValid,
                                                           latestValid,
                                                           earliestLead,
                                                           latestLead );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( inner );

        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( "DRRC2_DRRC2_DRRC2", geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( featureGroup,
                                            timeWindow,
                                            timeScale,
                                            threshold,
                                            false );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN )
                                                                                                                       .setUnits(
                                                                                                                               CMS ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN )
                                                                                                                       .setUnits(
                                                                                                                               CMS ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 3.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN )
                                                                                                                       .setUnits(
                                                                                                                               CMS ) ) )
                                    .build();

        return Statistics.newBuilder()
                         .setPool( pool )
                         .addScores( one )
                         .addScores( two )
                         .addScores( three )
                         .build();
    }

    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        if ( Objects.nonNull( WritingConsumerFactoryTest.broker ) )
        {
            WritingConsumerFactoryTest.broker.close();
        }
    }

}

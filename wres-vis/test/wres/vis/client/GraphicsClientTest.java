package wres.vis.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.Consumers;
import wres.events.Evaluation;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * Tests the {@link GraphicsClient}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class GraphicsClientTest
{
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
     * Connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    /**
     * Path to write.
     */

    private Path outputPath;

    @BeforeClass
    public static void runBeforeAllTests()
    {
        GraphicsClientTest.connections = BrokerConnectionFactory.of();
    }

    @Before
    public void runBeforeEachTest()
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setPng( PngFormat.getDefaultInstance() )
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
    public void publishAndConsumeOneEvaluationWithAnExternalGraphicsSubscriber() throws IOException, InterruptedException
    {
        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        List<wres.statistics.generated.Evaluation> actualEvaluations = new ArrayList<>();
        List<EvaluationStatus> actualStatuses = new ArrayList<>();
        List<Statistics> actualStatistics = new ArrayList<>();
        Set<Path> actualPathsWritten;

        // Internal consumers. Internal always means in-band to the evaluation process.
        Consumer<EvaluationStatus> status = actualStatuses::add;
        Consumer<wres.statistics.generated.Evaluation> description = actualEvaluations::add;
        Consumer<Statistics> statistics = actualStatistics::add;
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( status )
                                       .addEvaluationConsumer( description )
                                       .addStatisticsConsumer( statistics, new Format[] { Format.CSV } )
                                       .build();

        // Open an evaluation, closing on completion
        Path basePath = null;
        try ( // This is the graphics server instance, which normally runs in a separate process
              GraphicsClient graphics = GraphicsClient.of( GraphicsClientTest.connections ) )
        {
            // Start the graphics client
            graphics.start();
            
            try ( // This is the evaluation instance that declares png output
                  Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                         GraphicsClientTest.connections,
                                                         consumerGroup ); )
            {

                // Publish the statistics to a "feature" group
                evaluation.publish( this.oneStatistics, "DRRC2" );

                // Mark publication complete, which implicitly marks all groups complete
                evaluation.markPublicationCompleteReportedSuccess();

                // Wait for the evaluation to complete
                evaluation.await();

                // Record the paths written to assert against
                actualPathsWritten = evaluation.getPathsWrittenByExternalSubscribers();

                basePath = this.outputPath.resolve( "wres_evaluation_output_" + evaluation.getEvaluationId() );

            }
        }

        // Make assertions about the things produced by internal subscriptions.
        assertEquals( List.of( this.oneEvaluation ), actualEvaluations );
        assertEquals( List.of( this.oneStatistics ), actualStatistics );

        // Make assertions about the graphics written by the single external subscription.
        Set<Path> expectedPaths = Set.of( basePath.resolve( "DRRC2_DRRC2_DRRC2_HEFS_MEAN_ABSOLUTE_ERROR.png" ),
                                          basePath.resolve( "DRRC2_DRRC2_DRRC2_HEFS_MEAN_ERROR.png" ),
                                          basePath.resolve( "DRRC2_DRRC2_DRRC2_HEFS_MEAN_SQUARE_ERROR.png" ) );

        assertEquals( expectedPaths, actualPathsWritten );

        // Clean up by deleting the paths written
        for ( Path next : actualPathsWritten )
        {
            next.toFile().delete();
        }

        basePath.toFile().delete();
    }

    /**
     * @return several score statistics for one pool
     */

    private Statistics getScoreStatisticsForOnePool()
    {
        wres.datamodel.scale.TimeScaleOuter timeScale =
                wres.datamodel.scale.TimeScaleOuter.of( java.time.Duration.ofHours( 1 ),
                                                        wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction.MEAN );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( wres.datamodel.thresholds.ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                                    Operator.GREATER,
                                                                                    ThresholdDataType.LEFT ) );

        FeatureKey location = new FeatureKey( "DRRC2", null, null, "POINT ( 23.45, 56.21 )" );

        Instant earliestValid = Instant.parse( "2551-03-20T01:00:00Z" );
        Instant latestValid = Instant.parse( "2551-03-20T12:00:00Z" );
        Instant earliestReference = Instant.parse( "2551-03-19T00:00:00Z" );
        Instant latestReference = Instant.parse( "2551-03-19T12:00:00Z" );
        Duration earliestLead = Duration.ofHours( 1 );
        Duration latestLead = Duration.ofHours( 7 );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( earliestReference,
                                    latestReference,
                                    earliestValid,
                                    latestValid,
                                    earliestLead,
                                    latestLead );

        Pool pool = MessageFactory.parse( new FeatureTuple( location,
                                                            location,
                                                            location ),
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
                                                                                                                       .setName( ComponentName.MAIN )
                                                                                                                       .setUnits( CMS ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN )
                                                                                                                       .setUnits( CMS ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 3.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN )
                                                                                                                       .setUnits( CMS ) ) )
                                    .build();

        return Statistics.newBuilder()
                         .setPool( pool )
                         .addScores( one )
                         .addScores( two )
                         .addScores( three )
                         .build();
    }

    @AfterClass
    public static void runAfterAllTests() throws IOException
    {
        GraphicsClientTest.connections.close();
    }

}

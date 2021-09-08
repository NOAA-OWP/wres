package wres.vis.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.Evaluation;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
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
    public void publishAndConsumeOneEvaluationWithAnExternalGraphicsSubscriber()
            throws IOException, InterruptedException
    {
        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        Set<Path> actualPathsWritten;

        // Open an evaluation, closing on completion
        Path basePath = null;
        
        GraphicsClient graphics = GraphicsClient.of( GraphicsClientTest.connections );

        // Start the graphics client
        graphics.start();

        try ( // This is the evaluation instance that declares png output
              Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                     GraphicsClientTest.connections,
                                                     "aClient" ); )
        {

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
            graphics.stop();
        }

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

        FeatureTuple featureTuple = new FeatureTuple( location,
                                                      location,
                                                      location );

        FeatureGroup featureGroup = FeatureGroup.of( featureTuple );

        Pool pool = MessageFactory.parse( featureGroup,
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

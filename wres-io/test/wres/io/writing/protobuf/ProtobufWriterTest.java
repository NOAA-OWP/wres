package wres.io.writing.protobuf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;

import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ProtobufWriter}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ProtobufWriterTest
{

    /**A writer instance for testing.*/
    private ProtobufWriter writer;

    /**Temp directory for writing.*/
    private static final String TEMP_DIR = System.getProperty( "java.io.tmpdir" );

    /**Path to write.*/
    private static final Path PATH = Paths.get( ProtobufWriterTest.TEMP_DIR, "ProtobufWriterTest.pb3" );

    /**An evaluation to write.*/
    private Evaluation evaluation;

    @Before
    public void runBeforeEachTest()
    {
        this.evaluation = Evaluation.newBuilder()
                                    .setMetricMessageCount( 1 )
                                    .setPoolMessageCount( 2 )
                                    .setLeftSourceName( "aLeftSource" )
                                    .setRightSourceName( "aRightSource" )
                                    .setMeasurementUnit( "aMeasurementUnit" )
                                    .setTimeScale( TimeScale.newBuilder()
                                                            .setPeriod( Duration.newBuilder()
                                                                                .setSeconds( 77 ) ) )
                                    .setVariableName( "aVariable" )
                                    .build();

        this.writer = ProtobufWriter.of( ProtobufWriterTest.PATH, this.evaluation );
    }

    @Test
    public void testWriteOneEvaluationWithTwoPoolsOfStatistics() throws IOException
    {
        Statistics.Builder poolOne = Statistics.newBuilder();

        poolOne.setPool( Pool.newBuilder()
                             .setEventThreshold( Threshold.newBuilder()
                                                          .setLeftThresholdValue( DoubleValue.of( 12345 ) )
                                                          .setName( "Flooding" ) )
                             .addGeometryTuples( GeometryTuple.newBuilder()
                                                              .setLeft( Geometry.newBuilder()
                                                                                .setName( "DRRC2" ) ) )
                             .setTimeWindow( TimeWindow.newBuilder()
                                                       .setEarliestLeadDuration( Duration.newBuilder()
                                                                                         .setSeconds( 3600 ) )
                                                       .setLatestLeadDuration( Duration.newBuilder()
                                                                                       .setSeconds( 3600 ) ) ) );
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

        poolOne.addScores( one )
               .addScores( two )
               .addScores( three );

        // Write the first pool
        this.writer.accept( poolOne.build() );

        Statistics.Builder poolTwo = Statistics.newBuilder();

        poolTwo.setPool( Pool.newBuilder()
                             .setEventThreshold( Threshold.newBuilder()
                                                          .setLeftThresholdValue( DoubleValue.of( 12345 ) )
                                                          .setName( "Flooding" ) )
                             .addGeometryTuples( GeometryTuple.newBuilder()
                                                              .setLeft( Geometry.newBuilder()
                                                                                .setName( "DRRC2" ) ) )
                             .setTimeWindow( TimeWindow.newBuilder()
                                                       .setEarliestLeadDuration( Duration.newBuilder()
                                                                                         .setSeconds( 3600 ) )
                                                       .setLatestLeadDuration( Duration.newBuilder()
                                                                                       .setSeconds( 3600 ) ) ) );
        DoubleScoreStatistic four =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        DoubleScoreStatistic five =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        poolTwo.addScores( four )
               .addScores( five );

        // Write the second pool
        this.writer.accept( poolTwo.build() );

        // Read back in and assert equality for all messages
        try ( InputStream stream = Files.newInputStream( ProtobufWriterTest.PATH ) )
        {
            Evaluation actualEvaluation = Evaluation.parseDelimitedFrom( stream );

            assertEquals( this.evaluation, actualEvaluation );

            Statistics poolOneActual = Statistics.parseDelimitedFrom( stream );

            assertEquals( poolOne.build(), poolOneActual );

            Statistics poolTwoActual = Statistics.parseDelimitedFrom( stream );

            assertEquals( poolTwo.build(), poolTwoActual );
        }
    }

    @After
    public void runAfterEachTest() throws IOException
    {
        Files.deleteIfExists( ProtobufWriterTest.PATH );
    }

}

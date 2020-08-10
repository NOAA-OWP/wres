package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.processing.MetricFuturesByTime.MetricFuturesByTimeBuilder;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link MetricFuturesByTime}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricFuturesByTimeTest
{

    /**
     * A default instance to test.
     */

    private MetricFuturesByTime futures;

    /**
     * Default boxplot output.
     */

    private List<BoxplotStatisticOuter> boxplot;

    /**
     * Default double score output.
     */

    private List<DoubleScoreStatisticOuter> doubleScore;

    /**
     * Default duration score output.
     */

    private List<DurationScoreStatisticOuter> durationScore;

    /**
     * Default multivector output.
     */

    private List<DiagramStatisticOuter> multivector;

    /**
     * Default paired output.
     */

    private List<DurationDiagramStatisticOuter> paired;

    /**
     * A key used to store output.
     */

    Pair<TimeWindowOuter, OneOrTwoThresholds> key;

    @Before
    public void setupBeforeEachTest()
    {
        this.key = Pair.of( TimeWindowOuter.of( Instant.parse( "1985-05-01T12:00:00Z" ),
                                                Instant.parse( "1985-05-03T12:00:00Z" ) ),
                            OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                      Operator.GREATER,
                                                                      ThresholdDataType.LEFT ) ) );

        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        // Add a boxplot future
        this.boxplot =
                Collections.singletonList( BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                                     .setMetric( BoxplotMetric.newBuilder()
                                                                                                              .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
                                                                                     .build(),
                                                                     SampleMetadata.of() ) );

        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( boxplot ) );

        SampleMetadata doubleScoreMeta = SampleMetadata.of();

        this.doubleScore =
                Collections.singletonList( DoubleScoreStatisticOuter.of( DoubleScoreStatistic.newBuilder()
                                                                                             .setMetric( DoubleScoreMetric.newBuilder()
                                                                                                                          .setName( MetricName.COEFFICIENT_OF_DETERMINATION ) )
                                                                                             .build(),
                                                                         doubleScoreMeta ) );

        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );

        SampleMetadata dScoreMetadata = SampleMetadata.of();

        // Add a duration score future
        this.durationScore =
                Collections.singletonList( DurationScoreStatisticOuter.of( DurationScoreStatistic.newBuilder()
                                                                                                 .setMetric( DurationScoreMetric.newBuilder()
                                                                                                                                .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC ) )
                                                                                                 .build(),
                                                                           dScoreMetadata ) );

        builder.addDurationScoreOutput( CompletableFuture.completedFuture( durationScore ) );

        // Add multi-vector output
        this.multivector =
                Collections.singletonList( DiagramStatisticOuter.of( DiagramStatistic.newBuilder()
                                                                                     .setMetric( DiagramMetric.newBuilder()
                                                                                                              .setName( MetricName.COEFFICIENT_OF_DETERMINATION ) )
                                                                                     .build(),
                                                                     SampleMetadata.of() ) );

        builder.addDiagramOutput( CompletableFuture.completedFuture( multivector ) );

        // Add paired output
        this.paired =
                Collections.singletonList( DurationDiagramStatisticOuter.of( DurationDiagramStatistic.newBuilder()
                                                                                                     .setMetric( DurationDiagramMetric.newBuilder()
                                                                                                                                      .setName( MetricName.COEFFICIENT_OF_DETERMINATION ) )
                                                                                                     .build(),
                                                                             SampleMetadata.of() ) );

        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );

        this.futures = builder.build();
    }

    /**
     * Compares the {@link MetricFuturesByTime#getOutputTypes()} against expected output.
     */

    @Test
    public void testGetOutputTypes()
    {
        // Check with all present
        assertTrue( this.futures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( this.futures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( this.futures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( this.futures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertTrue( this.futures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );

        // Check with none present
        MetricFuturesByTime emptyFutures = new MetricFuturesByTimeBuilder().build();
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );
    }

    /**
     * Checks that the {@link MetricFuturesByTime#getMetricOutput()} returns non-null output when expected.
     */

    @Test
    public void testGetMetricOutput()
    {
        // Check with all present
        assertNotNull( this.futures.getMetricOutput() );
    }

    /**
     * Checks that the {@link MetricFuturesByTime#hasFutureOutputs()} returns the expected state.
     */

    @Test
    public void testHasFutureOutputs()
    {
        // Full futures
        assertTrue( this.futures.hasFutureOutputs() );

        // Empty futures
        MetricFuturesByTime emptyFutures = new MetricFuturesByTimeBuilder().build();
        assertFalse( emptyFutures.hasFutureOutputs() );
    }

    /**
     * Checks that a new result is added correctly if the builder already contains an existing result of the same type.
     */

    @Test
    public void testPutIfAbsentInBuilder()
    {
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();
        // Add once
        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( this.boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( this.durationScore ) );
        builder.addDiagramOutput( CompletableFuture.completedFuture( this.multivector ) );
        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );
        // Add again
        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( this.boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( this.durationScore ) );
        builder.addDiagramOutput( CompletableFuture.completedFuture( this.multivector ) );
        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );

        // Check all expected output is present
        MetricFuturesByTime metricFutures = builder.build();
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );
    }

    /**
     * Tests the merge of a {@link MetricFuturesByTime} with an existing builder.
     */

    @Test
    public void testMergeMetricFutures()
    {
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( this.boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( this.durationScore ) );
        builder.addDiagramOutput( CompletableFuture.completedFuture( this.multivector ) );
        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );
        builder.addFutures( this.futures );

        // Check all expected output is present
        MetricFuturesByTime metricFutures = builder.build();
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );
    }

}

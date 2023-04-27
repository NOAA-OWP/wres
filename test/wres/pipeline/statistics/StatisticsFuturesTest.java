package wres.pipeline.statistics;

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

import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.OneOrTwoDoubles;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.time.TimeWindowOuter;
import wres.pipeline.statistics.StatisticsFutures.MetricFuturesByTimeBuilder;
import wres.statistics.MessageFactory;
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
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link StatisticsFutures}.
 * 
 * @author James Brown
 */
public final class StatisticsFuturesTest
{

    /**
     * A default instance to test.
     */

    private StatisticsFutures futures;

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
     * Default diagram output.
     */

    private List<DiagramStatisticOuter> diagrams;

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
        TimeWindow timeWindow = MessageFactory.getTimeWindow( Instant.parse( "1985-05-01T12:00:00Z" ),
                                                              Instant.parse( "1985-05-03T12:00:00Z" ) );
        this.key = Pair.of( TimeWindowOuter.of( timeWindow ),
                            OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                      ThresholdOperator.GREATER,
                                                                      ThresholdOrientation.LEFT ) ) );

        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        // Add a boxplot future
        this.boxplot =
                Collections.singletonList( BoxplotStatisticOuter.of( BoxplotStatistic.newBuilder()
                                                                                     .setMetric( BoxplotMetric.newBuilder()
                                                                                                              .setName( MetricName.BOX_PLOT_OF_ERRORS ) )
                                                                                     .build(),
                                                                     PoolMetadata.of() ) );

        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( boxplot ) );

        PoolMetadata doubleScoreMeta = PoolMetadata.of();

        this.doubleScore =
                Collections.singletonList( DoubleScoreStatisticOuter.of( DoubleScoreStatistic.newBuilder()
                                                                                             .setMetric( DoubleScoreMetric.newBuilder()
                                                                                                                          .setName( MetricName.COEFFICIENT_OF_DETERMINATION ) )
                                                                                             .build(),
                                                                         doubleScoreMeta ) );

        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );

        PoolMetadata dScoreMetadata = PoolMetadata.of();

        // Add a duration score future
        this.durationScore =
                Collections.singletonList( DurationScoreStatisticOuter.of( DurationScoreStatistic.newBuilder()
                                                                                                 .setMetric( DurationScoreMetric.newBuilder()
                                                                                                                                .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC ) )
                                                                                                 .build(),
                                                                           dScoreMetadata ) );

        builder.addDurationScoreOutput( CompletableFuture.completedFuture( this.durationScore ) );

        // Add multi-vector output
        this.diagrams =
                Collections.singletonList( DiagramStatisticOuter.of( DiagramStatistic.newBuilder()
                                                                                     .setMetric( DiagramMetric.newBuilder()
                                                                                                              .setName( MetricName.COEFFICIENT_OF_DETERMINATION ) )
                                                                                     .build(),
                                                                     PoolMetadata.of() ) );

        builder.addDiagramOutput( CompletableFuture.completedFuture( this.diagrams ) );

        // Add paired output
        this.paired =
                Collections.singletonList( DurationDiagramStatisticOuter.of( DurationDiagramStatistic.newBuilder()
                                                                                                     .setMetric( DurationDiagramMetric.newBuilder()
                                                                                                                                      .setName( MetricName.COEFFICIENT_OF_DETERMINATION ) )
                                                                                                     .build(),
                                                                             PoolMetadata.of() ) );

        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );

        this.futures = builder.build();
    }

    /**
     * Compares the {@link StatisticsFutures#getOutputTypes()} against expected output.
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
        StatisticsFutures emptyFutures = new MetricFuturesByTimeBuilder().build();
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );
    }

    /**
     * Checks that the {@link StatisticsFutures#getMetricOutput()} returns non-null output when expected.
     */

    @Test
    public void testGetMetricOutput()
    {
        // Check with all present
        assertNotNull( this.futures.getMetricOutput() );
    }

    /**
     * Checks that the {@link StatisticsFutures#hasFutureOutputs()} returns the expected state.
     */

    @Test
    public void testHasFutureOutputs()
    {
        // Full futures
        assertTrue( this.futures.hasFutureOutputs() );

        // Empty futures
        StatisticsFutures emptyFutures = new MetricFuturesByTimeBuilder().build();
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
        builder.addDiagramOutput( CompletableFuture.completedFuture( this.diagrams ) );
        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );
        // Add again
        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( this.boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( this.durationScore ) );
        builder.addDiagramOutput( CompletableFuture.completedFuture( this.diagrams ) );
        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );

        // Check all expected output is present
        StatisticsFutures metricFutures = builder.build();
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );
    }

    /**
     * Tests the merge of a {@link StatisticsFutures} with an existing builder.
     */

    @Test
    public void testMergeMetricFutures()
    {
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( this.boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( this.doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( this.durationScore ) );
        builder.addDiagramOutput( CompletableFuture.completedFuture( this.diagrams ) );
        builder.addDurationDiagramOutput( CompletableFuture.completedFuture( this.paired ) );
        builder.addFutures( this.futures );

        // Check all expected output is present
        StatisticsFutures metricFutures = builder.build();
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DIAGRAM ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_DIAGRAM ) );
    }

}

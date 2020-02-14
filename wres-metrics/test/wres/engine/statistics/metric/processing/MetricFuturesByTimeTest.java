package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.processing.MetricFuturesByTime.MetricFuturesByTimeBuilder;

/**
 * Tests the {@link MetricFuturesByTime}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricFuturesByTimeTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * A default instance to test.
     */

    private MetricFuturesByTime futures;

    /**
     * Default boxplot output.
     */

    private ListOfStatistics<BoxPlotStatistics> boxplot;

    /**
     * Default double score output.
     */

    private ListOfStatistics<DoubleScoreStatistic> doubleScore;

    /**
     * Default duration score output.
     */

    private ListOfStatistics<DurationScoreStatistic> durationScore;

    /**
     * Default matrix output.
     */

    private ListOfStatistics<MatrixStatistic> matrix;

    /**
     * Default multivector output.
     */

    private ListOfStatistics<DiagramStatistic> multivector;

    /**
     * Default paired output.
     */

    private ListOfStatistics<PairedStatistic<Instant, Duration>> paired;

    /**
     * A key used to store output.
     */

    Pair<TimeWindow, OneOrTwoThresholds> key;

    @Before
    public void setupBeforeEachTest()
    {
        key = Pair.of( TimeWindow.of( Instant.parse( "1985-05-01T12:00:00Z" ),
                                      Instant.parse( "1985-05-03T12:00:00Z" ) ),
                       OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT ) ) );
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        // Add a boxplot future
        boxplot =
                ListOfStatistics.of( Collections.singletonList( BoxPlotStatistics.of( Arrays.asList(),
                                                                                      StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                                                                            1,
                                                                                                            MeasurementUnit.of(),
                                                                                                            MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                                                                            MetricConstants.MAIN ) ) ) );

        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( boxplot ) );

        // Add a double score future
        doubleScore =
                ListOfStatistics.of( Collections.singletonList( DoubleScoreStatistic.of( 1.0,
                                                                                         StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                                                                               1,
                                                                                                               MeasurementUnit.of(),
                                                                                                               MetricConstants.MEAN_ERROR,
                                                                                                               MetricConstants.MAIN ) ) ) );

        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( doubleScore ) );


        // Add a duration score future
        durationScore =
                ListOfStatistics.of( Collections.singletonList( DurationScoreStatistic.of( Duration.ofDays( 1 ),
                                                                                           StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                                                                                 1,
                                                                                                                 MeasurementUnit.of(),
                                                                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                                                                 MetricConstants.MAIN ) ) ) );

        builder.addDurationScoreOutput( CompletableFuture.completedFuture( durationScore ) );


        // Add a matrix output
        matrix =
                ListOfStatistics.of( Collections.singletonList( MatrixStatistic.of( new double[][] { { 1, 1 },
                                                                                                     { 1, 1 } },
                                                                                    StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                                                                          1,
                                                                                                          MeasurementUnit.of(),
                                                                                                          MetricConstants.CONTINGENCY_TABLE,
                                                                                                          MetricConstants.MAIN ) ) ) );

        builder.addMatrixOutput( CompletableFuture.completedFuture( matrix ) );

        // Add multi-vector output
        multivector =
                ListOfStatistics.of( Collections.singletonList( DiagramStatistic.of( Collections.singletonMap( MetricDimension.FORECAST_PROBABILITY,
                                                                                                               VectorOfDoubles.of( 1 ) ),
                                                                                     StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                                                                           1,
                                                                                                           MeasurementUnit.of(),
                                                                                                           MetricConstants.CONTINGENCY_TABLE,
                                                                                                           MetricConstants.MAIN ) ) ) );

        builder.addMultiVectorOutput( CompletableFuture.completedFuture( multivector ) );

        // Add paired output
        paired =
                ListOfStatistics.of( Collections.singletonList( PairedStatistic.of( Arrays.asList(),
                                                                                    StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                                                                          1,
                                                                                                          MeasurementUnit.of(),
                                                                                                          MetricConstants.CONTINGENCY_TABLE,
                                                                                                          MetricConstants.MAIN ) ) ) );

        builder.addPairedOutput( CompletableFuture.completedFuture( paired ) );

        futures = builder.build();
    }

    /**
     * Compares the {@link MetricFuturesByTime#getOutputTypes()} against expected output.
     */

    @Test
    public void testGetOutputTypes()
    {
        // Check with all present
        assertTrue( futures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( futures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( StatisticType.MATRIX ) );
        assertTrue( futures.getOutputTypes().contains( StatisticType.MULTIVECTOR ) );
        assertTrue( futures.getOutputTypes().contains( StatisticType.PAIRED ) );

        // Check with none present
        MetricFuturesByTime emptyFutures = new MetricFuturesByTimeBuilder().build();
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.MATRIX ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.MULTIVECTOR ) );
        assertFalse( emptyFutures.getOutputTypes().contains( StatisticType.PAIRED ) );
    }

    /**
     * Checks that the {@link MetricFuturesByTime#getMetricOutput()} returns non-null output when expected.
     */

    @Test
    public void testGetMetricOutput()
    {
        // Check with all present
        assertNotNull( futures.getMetricOutput() );
    }

    /**
     * Checks that the {@link MetricFuturesByTime#hasFutureOutputs()} returns the expected state.
     */

    @Test
    public void testHasFutureOutputs()
    {
        // Full futures
        assertTrue( futures.hasFutureOutputs() );

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
        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( durationScore ) );
        builder.addMatrixOutput( CompletableFuture.completedFuture( matrix ) );
        builder.addMultiVectorOutput( CompletableFuture.completedFuture( multivector ) );
        builder.addPairedOutput( CompletableFuture.completedFuture( paired ) );
        // Add again
        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( durationScore ) );
        builder.addMatrixOutput( CompletableFuture.completedFuture( matrix ) );
        builder.addMultiVectorOutput( CompletableFuture.completedFuture( multivector ) );
        builder.addPairedOutput( CompletableFuture.completedFuture( paired ) );

        // Check all expected output is present
        MetricFuturesByTime metricFutures = builder.build();
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.MATRIX ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.MULTIVECTOR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.PAIRED ) );
    }

    /**
     * Tests the merge of a {@link MetricFuturesByTime} with an existing builder.
     */

    @Test
    public void testMergeMetricFutures()
    {
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        builder.addBoxPlotOutputPerPair( CompletableFuture.completedFuture( boxplot ) );
        builder.addDoubleScoreOutput( CompletableFuture.completedFuture( doubleScore ) );
        builder.addDurationScoreOutput( CompletableFuture.completedFuture( durationScore ) );
        builder.addMatrixOutput( CompletableFuture.completedFuture( matrix ) );
        builder.addMultiVectorOutput( CompletableFuture.completedFuture( multivector ) );
        builder.addPairedOutput( CompletableFuture.completedFuture( paired ) );
        builder.addFutures( this.futures );

        // Check all expected output is present
        MetricFuturesByTime metricFutures = builder.build();
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.BOXPLOT_PER_PAIR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DOUBLE_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.DURATION_SCORE ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.MATRIX ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.MULTIVECTOR ) );
        assertTrue( metricFutures.getOutputTypes().contains( StatisticType.PAIRED ) );
    }

}

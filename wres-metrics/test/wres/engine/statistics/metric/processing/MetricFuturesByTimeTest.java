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
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotOutput;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.datamodel.statistics.DurationScoreOutput;
import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.statistics.MatrixOutput;
import wres.datamodel.statistics.MultiVectorOutput;
import wres.datamodel.statistics.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
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

    private ListOfMetricOutput<BoxPlotOutput> boxplot;

    /**
     * Default double score output.
     */

    private ListOfMetricOutput<DoubleScoreOutput> doubleScore;

    /**
     * Default duration score output.
     */

    private ListOfMetricOutput<DurationScoreOutput> durationScore;

    /**
     * Default matrix output.
     */

    private ListOfMetricOutput<MatrixOutput> matrix;

    /**
     * Default multivector output.
     */

    private ListOfMetricOutput<MultiVectorOutput> multivector;

    /**
     * Default paired output.
     */

    private ListOfMetricOutput<PairedOutput<Instant, Duration>> paired;

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
                ListOfMetricOutput.of( Collections.singletonList( BoxPlotOutput.of( Arrays.asList(),
                                                                                    VectorOfDoubles.of( new double[] { 0.1,
                                                                                                                       0.9 } ),
                                                                                    MetricOutputMetadata.of( 1,
                                                                                                             MeasurementUnit.of(),
                                                                                                             MeasurementUnit.of(),
                                                                                                             MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                                                                             MetricConstants.MAIN ),
                                                                                    MetricDimension.OBSERVED_VALUE,
                                                                                    MetricDimension.FORECAST_ERROR ) ) );

        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( boxplot ) );

        // Add a double score future
        doubleScore =
                ListOfMetricOutput.of( Collections.singletonList( DoubleScoreOutput.of( 1.0,
                                                                                        MetricOutputMetadata.of( 1,
                                                                                                                 MeasurementUnit.of(),
                                                                                                                 MeasurementUnit.of(),
                                                                                                                 MetricConstants.MEAN_ERROR,
                                                                                                                 MetricConstants.MAIN ) ) ) );

        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( doubleScore ) );


        // Add a duration score future
        durationScore =
                ListOfMetricOutput.of( Collections.singletonList( DurationScoreOutput.of( Duration.ofDays( 1 ),
                                                                                          MetricOutputMetadata.of( 1,
                                                                                                                   MeasurementUnit.of(),
                                                                                                                   MeasurementUnit.of(),
                                                                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                                                                   MetricConstants.MAIN ) ) ) );

        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( durationScore ) );


        // Add a matrix output
        matrix =
                ListOfMetricOutput.of( Collections.singletonList( MatrixOutput.of( new double[][] { { 1, 1 },
                                                                                                    { 1, 1 } },
                                                                                   MetricOutputMetadata.of( 1,
                                                                                                            MeasurementUnit.of(),
                                                                                                            MeasurementUnit.of(),
                                                                                                            MetricConstants.CONTINGENCY_TABLE,
                                                                                                            MetricConstants.MAIN ) ) ) );

        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( matrix ) );

        // Add multi-vector output
        multivector =
                ListOfMetricOutput.of( Collections.singletonList( MultiVectorOutput.ofMultiVectorOutput( Collections.singletonMap( MetricDimension.FORECAST_PROBABILITY,
                                                                                                                                   new double[] { 1 } ),
                                                                                                         MetricOutputMetadata.of( 1,
                                                                                                                                  MeasurementUnit.of(),
                                                                                                                                  MeasurementUnit.of(),
                                                                                                                                  MetricConstants.CONTINGENCY_TABLE,
                                                                                                                                  MetricConstants.MAIN ) ) ) );

        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( multivector ) );

        // Add paired output
        paired =
                ListOfMetricOutput.of( Collections.singletonList( PairedOutput.of( Arrays.asList(),
                                                                                   MetricOutputMetadata.of( 1,
                                                                                                            MeasurementUnit.of(),
                                                                                                            MeasurementUnit.of(),
                                                                                                            MetricConstants.CONTINGENCY_TABLE,
                                                                                                            MetricConstants.MAIN ) ) ) );

        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( paired ) );

        futures = builder.build();
    }

    /**
     * Compares the {@link MetricFuturesByTime#getOutputTypes()} against expected output.
     */

    @Test
    public void testGetOutputTypes()
    {
        // Check with all present
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.BOXPLOT ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.DOUBLE_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.DURATION_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.MATRIX ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.MULTIVECTOR ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.PAIRED ) );

        // Check with none present
        MetricFuturesByTime emptyFutures = new MetricFuturesByTimeBuilder().build();
        assertFalse( emptyFutures.getOutputTypes().contains( MetricOutputGroup.BOXPLOT ) );
        assertFalse( emptyFutures.getOutputTypes().contains( MetricOutputGroup.DOUBLE_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( MetricOutputGroup.DURATION_SCORE ) );
        assertFalse( emptyFutures.getOutputTypes().contains( MetricOutputGroup.MATRIX ) );
        assertFalse( emptyFutures.getOutputTypes().contains( MetricOutputGroup.MULTIVECTOR ) );
        assertFalse( emptyFutures.getOutputTypes().contains( MetricOutputGroup.PAIRED ) );
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
        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( boxplot ) );
        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( doubleScore ) );
        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( durationScore ) );
        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( matrix ) );
        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( multivector ) );
        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( paired ) );
        // Add again
        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( boxplot ) );
        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( doubleScore ) );
        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( durationScore ) );
        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( matrix ) );
        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( multivector ) );
        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( paired ) );

        // Check all expected output is present
        MetricFuturesByTime futures = builder.build();
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.BOXPLOT ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.DOUBLE_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.DURATION_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.MATRIX ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.MULTIVECTOR ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.PAIRED ) );
    }

    /**
     * Tests the merge of a {@link MetricFuturesByTime} with an existing builder.
     */

    @Test
    public void testMergeMetricFutures()
    {
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        // Create a new key
        Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( TimeWindow.of( Instant.parse( "1985-05-01T12:00:00Z" ),
                                                                           Instant.parse( "1985-05-03T12:00:00Z" ) ),
                                                            OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 2.0 ),
                                                                                                 Operator.GREATER,
                                                                                                 ThresholdDataType.LEFT ) ) );

        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( boxplot ) );
        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( doubleScore ) );
        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( durationScore ) );
        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( matrix ) );
        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( multivector ) );
        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( paired ) );
        builder.addFutures( this.futures );

        // Check all expected output is present
        MetricFuturesByTime futures = builder.build();
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.BOXPLOT ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.DOUBLE_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.DURATION_SCORE ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.MATRIX ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.MULTIVECTOR ) );
        assertTrue( futures.getOutputTypes().contains( MetricOutputGroup.PAIRED ) );
    }

}

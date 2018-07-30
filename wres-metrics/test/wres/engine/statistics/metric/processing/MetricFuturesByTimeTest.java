package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
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

    private Map<MetricConstants, BoxPlotOutput> boxplot;

    /**
     * Default double score output.
     */

    private Map<MetricConstants, DoubleScoreOutput> doubleScore;

    /**
     * Default duration score output.
     */

    private Map<MetricConstants, DurationScoreOutput> durationScore;

    /**
     * Default matrix output.
     */

    private Map<MetricConstants, MatrixOutput> matrix;

    /**
     * Default multivector output.
     */

    private Map<MetricConstants, MultiVectorOutput> multivector;

    /**
     * Default paired output.
     */

    private Map<MetricConstants, PairedOutput<Instant, Duration>> paired;

    /**
     * A key used to store output.
     */

    Pair<TimeWindow, OneOrTwoThresholds> key;

    @Before
    public void setupBeforeEachTest()
    {
        key = Pair.of( TimeWindow.of( Instant.parse( "1985-05-01T12:00:00Z" ),
                                      Instant.parse( "1985-05-03T12:00:00Z" ) ),
                       OneOrTwoThresholds.of( DataFactory.ofThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ) ) );
        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        // Add a boxplot future
        boxplot =
                Collections.singletonMap( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                          DataFactory.ofBoxPlotOutput( Arrays.asList(),
                                                                       VectorOfDoubles.of( new double[] { 0.1,
                                                                                                          0.9 } ),
                                                                       MetadataFactory.getOutputMetadata( 1,
                                                                                                          MetadataFactory.getDimension(),
                                                                                                          MetadataFactory.getDimension(),
                                                                                                          MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ),
                                                                       MetricDimension.OBSERVED_VALUE,
                                                                       MetricDimension.FORECAST_ERROR ) );

        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( boxplot ) ) );

        // Add a double score future
        doubleScore =
                Collections.singletonMap( MetricConstants.MEAN_ERROR,
                                          DataFactory.ofDoubleScoreOutput( 1.0,
                                                                           MetadataFactory.getOutputMetadata( 1,
                                                                                                              MetadataFactory.getDimension(),
                                                                                                              MetadataFactory.getDimension(),
                                                                                                              MetricConstants.MEAN_ERROR ) ) );

        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( doubleScore ) ) );


        // Add a duration score future
        durationScore =
                Collections.singletonMap( MetricConstants.TIME_TO_PEAK_ERROR,
                                          DataFactory.ofDurationScoreOutput( Duration.ofDays( 1 ),
                                                                             MetadataFactory.getOutputMetadata( 1,
                                                                                                                MetadataFactory.getDimension(),
                                                                                                                MetadataFactory.getDimension(),
                                                                                                                MetricConstants.TIME_TO_PEAK_ERROR ) ) );

        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( durationScore ) ) );


        // Add a matrix output
        matrix =
                Collections.singletonMap( MetricConstants.CONTINGENCY_TABLE,
                                          DataFactory.ofMatrixOutput( new double[][] { { 1, 1 }, { 1, 1 } },
                                                                      MetadataFactory.getOutputMetadata( 1,
                                                                                                         MetadataFactory.getDimension(),
                                                                                                         MetadataFactory.getDimension(),
                                                                                                         MetricConstants.CONTINGENCY_TABLE ) ) );

        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( matrix ) ) );

        // Add multi-vector output
        multivector =
                Collections.singletonMap( MetricConstants.CONTINGENCY_TABLE,
                                          DataFactory.ofMultiVectorOutput( Collections.singletonMap( MetricDimension.FORECAST_PROBABILITY,
                                                                                                     new double[] { 1 } ),
                                                                           MetadataFactory.getOutputMetadata( 1,
                                                                                                              MetadataFactory.getDimension(),
                                                                                                              MetadataFactory.getDimension(),
                                                                                                              MetricConstants.CONTINGENCY_TABLE ) ) );

        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( multivector ) ) );

        // Add paired output
        paired =
                Collections.singletonMap( MetricConstants.CONTINGENCY_TABLE,
                                          DataFactory.ofPairedOutput( Arrays.asList(),
                                                                      MetadataFactory.getOutputMetadata( 1,
                                                                                                         MetadataFactory.getDimension(),
                                                                                                         MetadataFactory.getDimension(),
                                                                                                         MetricConstants.CONTINGENCY_TABLE ) ) );

        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( paired ) ) );

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
                                  CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( boxplot ) ) );
        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( doubleScore ) ) );
        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( durationScore ) ) );
        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( matrix ) ) );
        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( multivector ) ) );
        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( paired ) ) );
        // Add again
        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( boxplot ) ) );
        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( doubleScore ) ) );
        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( durationScore ) ) );
        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( matrix ) ) );
        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( multivector ) ) );
        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( paired ) ) );

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
                                                            OneOrTwoThresholds.of( DataFactory.ofThreshold( OneOrTwoDoubles.of( 2.0 ),
                                                                                                            Operator.GREATER,
                                                                                                            ThresholdDataType.LEFT ) ) );

        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( boxplot ) ) );
        builder.addDoubleScoreOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( doubleScore ) ) );
        builder.addDurationScoreOutput( key,
                                        CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( durationScore ) ) );
        builder.addMatrixOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( matrix ) ) );
        builder.addMultiVectorOutput( key,
                                      CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( multivector ) ) );
        builder.addPairedOutput( key,
                                 CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( paired ) ) );
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

    /**
     * Checks that an exception is thrown when attempting to merge a {@link MetricFuturesByTime} into an existing 
     * builder that contains one or more results for the same key.
     */

    @Test
    public void testMergeOverwriteThrowsException()
    {
        exception.expect( MetricOutputMergeException.class );
        exception.expectMessage( "A metric result already exists in this processor for metric output group 'BOXPLOT' "
                                 + "at time window '[1985-05-01T12:00:00Z, 1985-05-03T12:00:00Z, VALID TIME, PT0S, PT0S]' and threshold "
                                 + "'> 1.0': change the input data or corresponding metadata to ensure that a unique time window and "
                                 + "threshold is provided for each metric output." );

        MetricFuturesByTimeBuilder builder = new MetricFuturesByTimeBuilder();

        builder.addBoxPlotOutput( key,
                                  CompletableFuture.completedFuture( DataFactory.ofMetricOutputMapByMetric( boxplot ) ) );

        builder.addFutures( this.futures );
    }

}

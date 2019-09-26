package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.singlevalued.MeanError;

/**
 * Tests the {@link MetricTask}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricTaskTest
{
    /**
     * Executor service.
     */

    private ExecutorService pairPool;

    @Before
    public void setUpBeforeEachTest()
    {
        pairPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
    }

    /**
     * Constructs a {@link MetricTask} and compares the actual output to the expected output.
     * @throws MetricParameterException if the metric could not be constructed 
     * @throws ExecutionException if the task failed
     * @throws InterruptedException if the task was interrupted
     * @throws MetricCalculationException if the metric calculation failed
     */

    @Test
    public void testMetricTask()
            throws InterruptedException, ExecutionException
    {
        // Generate some data
        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Add some appropriate metrics to the collection
        final Metric<SampleData<Pair<Double, Double>>, DoubleScoreStatistic> m = MeanError.of();

        // Wrap an input in a future
        final FutureTask<SampleData<Pair<Double, Double>>> futureInput =
                new FutureTask<>( () -> input );

        final MetricTask<SampleData<Pair<Double, Double>>, DoubleScoreStatistic> task =
                new MetricTask<>( m, futureInput );

        // Compute the pairs
        pairPool.submit( futureInput );

        //Should not throw an exception
        StatisticMetadata benchmarkMeta = StatisticMetadata.of( input.getMetadata(),
                                                                10,
                                                                MeasurementUnit.of(),
                                                                MetricConstants.MEAN_ERROR,
                                                                MetricConstants.MAIN );
        DoubleScoreStatistic benchmark = DoubleScoreStatistic.of( 200.55, benchmarkMeta );

        assertTrue( benchmark.equals( task.call() ) );

    }

    /**
     * Constructs a {@link MetricTask} and expects an exception.
     * @throws MetricParameterException if the metric could not be constructed 
     * @throws ExecutionException if the task failed
     * @throws InterruptedException if the task was interrupted
     * @throws MetricCalculationException if the metric calculation failed
     */

    @Test
    public void testMetricTaskWithExceptionalResult()
            throws InterruptedException, ExecutionException
    {

        // Add some appropriate metrics to the collection
        final Metric<SampleData<Pair<Double, Double>>, DoubleScoreStatistic> m = MeanError.of();

        final FutureTask<SampleData<Pair<Double, Double>>> futureInputNull =
                new FutureTask<>( () -> null );

        // Compute the pairs
        pairPool.submit( futureInputNull );

        // Exceptional case
        MetricTask<SampleData<Pair<Double, Double>>, DoubleScoreStatistic> task2 =
                new MetricTask<>( m, futureInputNull );

        // Unrecognized metric
        MetricCalculationException expected = assertThrows( MetricCalculationException.class,
                                                            () -> task2.call() );
        assertEquals( "Cannot compute a metric with null input.", expected.getMessage() );
    }

    @After
    public void tearDownAfterEachTest()
    {
        if ( Objects.nonNull( pairPool ) )
        {
            pairPool.shutdownNow();
        }
    }

}

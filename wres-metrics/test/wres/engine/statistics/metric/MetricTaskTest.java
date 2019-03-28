package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.singlevalued.MeanError;

/**
 * Tests the {@link MetricTask}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricTaskTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

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
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Add some appropriate metrics to the collection
        final Metric<SingleValuedPairs, DoubleScoreStatistic> m = MeanError.of();

        // Wrap an input in a future
        final FutureTask<SingleValuedPairs> futureInput =
                new FutureTask<>( () -> input );

        final MetricTask<SingleValuedPairs, DoubleScoreStatistic> task = new MetricTask<>( m, futureInput );

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
        final Metric<SingleValuedPairs, DoubleScoreStatistic> m = MeanError.of();

        final FutureTask<SingleValuedPairs> futureInputNull =
                new FutureTask<>( () -> null );

        // Compute the pairs
        pairPool.submit( futureInputNull );

        // Exceptional case
        MetricTask<SingleValuedPairs, DoubleScoreStatistic> task2 = new MetricTask<>( m, futureInputNull );
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Cannot compute a metric with null input." );
        task2.call();
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

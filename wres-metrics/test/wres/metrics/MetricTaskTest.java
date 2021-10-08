package wres.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.singlevalued.MeanError;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;

/**
 * Tests the {@link MetricTask}.
 * 
 * @author James Brown
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
        final Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Add some appropriate metrics to the collection
        final Metric<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter> m = MeanError.of();

        // Wrap an input in a future
        final FutureTask<Pool<Pair<Double, Double>>> futureInput =
                new FutureTask<>( () -> input );

        final MetricTask<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter> task =
                new MetricTask<>( m, futureInput );

        // Compute the pairs
        this.pairPool.submit( futureInput );

        DoubleScoreMetricComponent metricComponent = MeanError.METRIC_INNER.getComponents( 0 )
                                                                     .toBuilder()
                                                                     .setUnits( input.getMetadata()
                                                                                     .getMeasurementUnit()
                                                                                     .toString() )
                                                                     .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 200.55 )
                                                                               .build();
        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( DoubleScoreMetric.newBuilder()
                                                                                      .setName( MetricName.MEAN_ERROR ) )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter benchmark = DoubleScoreStatisticOuter.of( score, input.getMetadata() );

        assertEquals( benchmark, task.call() );
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
        final Metric<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter> m = MeanError.of();

        final FutureTask<Pool<Pair<Double, Double>>> futureInputNull =
                new FutureTask<>( () -> null );

        // Compute the pairs
        pairPool.submit( futureInputNull );

        // Exceptional case
        MetricTask<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter> task2 =
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

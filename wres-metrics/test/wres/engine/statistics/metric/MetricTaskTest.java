package wres.engine.statistics.metric;

import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;

/**
 * Tests the {@link MetricTask}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricTaskTest
{

    /**
     * Constructs a {@link MetricTask} and checks for exceptions.
     */

    @Test
    public void test1MetricTask()
    {
        
        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try
        {

            //Generate some data
            final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

            //Add some appropriate metrics to the collection
            final DataFactory outF = DefaultDataFactory.getInstance();
            final MetricFactory metF = MetricFactory.getInstance(outF);               
            final Metric<SingleValuedPairs, ScalarOutput> m = metF.ofMeanError();

            //Wrap an input in a future
            final FutureTask<SingleValuedPairs> futureInput =
                                                            new FutureTask<SingleValuedPairs>(new Callable<SingleValuedPairs>()
                                                            {
                                                                public SingleValuedPairs call()
                                                                {
                                                                    return input;
                                                                }
                                                            });
            final FutureTask<SingleValuedPairs> futureInputNull =
                                                                new FutureTask<SingleValuedPairs>(new Callable<SingleValuedPairs>()
                                                                {
                                                                    public SingleValuedPairs call()
                                                                    {
                                                                        return null;
                                                                    }
                                                                });
            final MetricTask<SingleValuedPairs, ScalarOutput> task = new MetricTask<>(m, futureInput);

            //Compute the pairs
            pairPool.submit(futureInput);
            pairPool.submit(futureInputNull);

            //Should not throw an exception
            try
            {
                task.call();
            }
            catch(final Exception e)
            {
                fail("Unexpected exception on calling metric task: " + e.getMessage() + ".");
            }
            //Should throw an exception
            try
            {
                final MetricTask<SingleValuedPairs, ScalarOutput> task2 = new MetricTask<>(m, futureInputNull);
                task2.call();
                fail("Expected an exception on calling metric task with null future input.");
            }
            catch(final Exception e)
            {
            }
        }
        finally
        {
            pairPool.shutdown();
        }

    }

}

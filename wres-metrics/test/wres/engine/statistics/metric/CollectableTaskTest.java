package wres.engine.statistics.metric;

import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;

/**
 * Tests the {@link CollectableTask}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CollectableTaskTest
{

    /**
     * Constructs a {@link CollectableTask} and checks for exceptions.
     */

    @Test
    public void test1CollectableTask()
    {
        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try
        {

            //Add some appropriate metrics to the collection
            final Collectable<DichotomousPairs, MetricOutput<?>, ScalarOutput> m =
                                                                                 MetricFactory.ofCriticalSuccessIndex();

            //Metadata for the output
            final MetricOutputMetadata m1 = MetadataFactory.getMetadata(100,
                                                                        MetadataFactory.getDimension(),
                                                                        MetricConstants.CONTINGENCY_TABLE,
                                                                        MetricConstants.MAIN,
                                                                        null,
                                                                        null);

            //Wrap an input in a future
            final FutureTask<MetricOutput<?>> futureInput =
                                                          new FutureTask<MetricOutput<?>>(new Callable<MetricOutput<?>>()
                                                          {
                                                              public MatrixOutput call()
                                                              {
                                                                  final double[][] returnMe = new double[][]{{1.0, 1.0},
                                                                      {1.0, 1.0}};
                                                                  return MetricOutputFactory.ofMatrixOutput(returnMe,
                                                                                                            m1);
                                                              }
                                                          });
            final FutureTask<MetricOutput<?>> futureInputNull =
                                                              new FutureTask<MetricOutput<?>>(new Callable<MetricOutput<?>>()
                                                              {
                                                                  public MatrixOutput call()
                                                                  {
                                                                      return null;
                                                                  }
                                                              });

            final CollectableTask<DichotomousPairs, MetricOutput<?>, ScalarOutput> task =
                                                                                        new CollectableTask<>(m,
                                                                                                              futureInput);

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
                final CollectableTask<DichotomousPairs, MetricOutput<?>, ScalarOutput> task2 =
                                                                                             new CollectableTask<>(m,
                                                                                                                   futureInputNull);
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

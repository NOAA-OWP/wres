package wres.engine.statistics.metric;

import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.MatrixOutput;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;

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

        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try
        {

            //Add some appropriate metrics to the collection
            final Collectable<DichotomousPairs, MatrixOutput, ScalarOutput> m = metF.ofCriticalSuccessIndex();

            //Metadata for the output
            final MetricOutputMetadata m1 = metaFac.getOutputMetadata(100,
                                                                metaFac.getDimension(),
                                                                metaFac.getDimension(),
                                                                MetricConstants.CONTINGENCY_TABLE,
                                                                MetricConstants.MAIN);

            //Wrap an input in a future
            final FutureTask<MatrixOutput> futureInput = new FutureTask<MatrixOutput>(new Callable<MatrixOutput>()
            {
                public MatrixOutput call()
                {
                    final double[][] returnMe = new double[][]{{1.0, 1.0}, {1.0, 1.0}};
                    return outF.ofMatrixOutput(returnMe, m1);
                }
            });
            final FutureTask<MatrixOutput> futureInputNull = new FutureTask<MatrixOutput>(new Callable<MatrixOutput>()
            {
                public MatrixOutput call()
                {
                    return null;
                }
            });

            final CollectableTask<DichotomousPairs, MatrixOutput, ScalarOutput> task =
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
                final CollectableTask<DichotomousPairs, MatrixOutput, ScalarOutput> task2 =
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

package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;

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
            throws MetricParameterException, MetricCalculationException, InterruptedException, ExecutionException
    {
        // Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Add some appropriate metrics to the collection
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final Metric<SingleValuedPairs, DoubleScoreOutput> m = metF.ofMeanError();

        // Wrap an input in a future
        final FutureTask<SingleValuedPairs> futureInput =
                new FutureTask<SingleValuedPairs>( new Callable<SingleValuedPairs>()
                {
                    public SingleValuedPairs call()
                    {
                        return input;
                    }
                } );

        final MetricTask<SingleValuedPairs, DoubleScoreOutput> task = new MetricTask<>( m, futureInput );

        // Compute the pairs
        pairPool.submit( futureInput );

        //Should not throw an exception
        MetricOutputMetadata benchmarkMeta = outF.getMetadataFactory().getOutputMetadata( 10,
                                                                                          outF.getMetadataFactory()
                                                                                              .getDimension(),
                                                                                          input.getMetadata(),
                                                                                          MetricConstants.MEAN_ERROR,
                                                                                          MetricConstants.MAIN );
        DoubleScoreOutput benchmark = outF.ofDoubleScoreOutput( 200.55, benchmarkMeta );

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
            throws MetricParameterException, MetricCalculationException, InterruptedException, ExecutionException
    {

        // Add some appropriate metrics to the collection
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final Metric<SingleValuedPairs, DoubleScoreOutput> m = metF.ofMeanError();

        final FutureTask<SingleValuedPairs> futureInputNull =
                new FutureTask<SingleValuedPairs>( new Callable<SingleValuedPairs>()
                {
                    public SingleValuedPairs call()
                    {
                        return null;
                    }
                } );

        // Compute the pairs
        pairPool.submit( futureInputNull );
        
        // Exceptional case
        MetricTask<SingleValuedPairs, DoubleScoreOutput> task2 = new MetricTask<>( m, futureInputNull );
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

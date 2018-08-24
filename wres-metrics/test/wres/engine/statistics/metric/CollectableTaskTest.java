package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;

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

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.datamodel.statistics.MatrixOutput;
import wres.engine.statistics.metric.categorical.ThreatScore;

/**
 * Tests the {@link CollectableTask}.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 */
public final class CollectableTaskTest
{
    private static final double DOUBLE_COMPARE_THRESHOLD = 0.00001;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private ExecutorService pairPool;
    private Collectable<DichotomousPairs, MatrixOutput, DoubleScoreOutput> m;

    /** 
     * Metadata for the output 
     */

    private MetricOutputMetadata m1;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        // Tests can run simultaneously, use only 1 (additional) Thread per test
        pairPool = Executors.newFixedThreadPool( 1 );
        //Add some appropriate metrics to the collection
        m = ThreatScore.of();

        m1 = MetricOutputMetadata.of( 100,
                                                MeasurementUnit.of(),
                                                MeasurementUnit.of(),
                                                MetricConstants.CONTINGENCY_TABLE,
                                                MetricConstants.MAIN );
    }

    @Test
    public void testCollectableTask() throws ExecutionException, InterruptedException
    {
        //Wrap an input in a future
        final FutureTask<MatrixOutput> futureInput =
                new FutureTask<MatrixOutput>( new Callable<MatrixOutput>()
                {
                    public MatrixOutput call()
                    {
                        final double[][] returnMe =
                                new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } };
                        return MatrixOutput.of( returnMe, m1 );
                    }
                } );

        CollectableTask<DichotomousPairs, MatrixOutput, DoubleScoreOutput> task =
                new CollectableTask<>( m,
                                       futureInput );

        //Compute the pairs
        pairPool.submit( futureInput );

        //Should not throw an exception
        DoubleScoreOutput output = task.call();

        assertEquals( 0.333333, output.getData(), DOUBLE_COMPARE_THRESHOLD );
    }

    @Test
    public void testExceptionOnNullInput() throws ExecutionException, InterruptedException
    {
        final FutureTask<MatrixOutput> futureInputNull =
                new FutureTask<MatrixOutput>( new Callable<MatrixOutput>()
                {
                    public MatrixOutput call()
                    {
                        return null;
                    }
                } );

        pairPool.submit( futureInputNull );

        final CollectableTask<DichotomousPairs, MatrixOutput, DoubleScoreOutput> task2 =
                new CollectableTask<>( m,
                                       futureInputNull );

        //Should throw an exception
        exception.expect( MetricInputException.class );
        task2.call();
    }

    @After
    public void tearDownAfterEachTest()
    {
        pairPool.shutdownNow();
    }
}

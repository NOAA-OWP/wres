package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;

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

    private DataFactory outF;
    private MetadataFactory metaFac;
    private MetricFactory metF;
    private ExecutorService pairPool;
    private Collectable<DichotomousPairs, MatrixOutput, DoubleScoreOutput> m;

    /** Metadata for the output */
    private MetricOutputMetadata m1;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        outF = DefaultDataFactory.getInstance();
        metaFac = outF.getMetadataFactory();
        metF = MetricFactory.getInstance( outF );
        // Tests can run simultaneously, use only 1 (additional) Thread per test
        pairPool = Executors.newFixedThreadPool( 1 );
        //Add some appropriate metrics to the collection
        m = metF.ofCriticalSuccessIndex();

        m1 = metaFac.getOutputMetadata( 100,
                                        metaFac.getDimension(),
                                        metaFac.getDimension(),
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
                        return outF.ofMatrixOutput( returnMe, m1 );
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
                new FutureTask<MatrixOutput>(new Callable<MatrixOutput>()
                {
                    public MatrixOutput call()
                    {
                        return null;
                    }
                });

        pairPool.submit( futureInputNull );

        final CollectableTask<DichotomousPairs, MatrixOutput, DoubleScoreOutput>
                task2 =
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

package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.StatisticMetadata;
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

    private ExecutorService pairPool;
    private Collectable<SampleData<Pair<Boolean,Boolean>>, MatrixStatistic, DoubleScoreStatistic> m;

    /** 
     * Metadata for the output 
     */

    private StatisticMetadata m1;

    @Before
    public void setupBeforeEachTest()
    {
        // Tests can run simultaneously, use only 1 (additional) Thread per test
        pairPool = Executors.newFixedThreadPool( 1 );
        //Add some appropriate metrics to the collection
        m = ThreatScore.of();

        m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                   100,
                                   MeasurementUnit.of(),
                                   MetricConstants.CONTINGENCY_TABLE,
                                   MetricConstants.MAIN );
    }

    @Test
    public void testCollectableTask() throws ExecutionException, InterruptedException
    {
        //Wrap an input in a future
        final FutureTask<MatrixStatistic> futureInput =
                new FutureTask<>( () -> {
                    final double[][] returnMe =
                            new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } };
                    return MatrixStatistic.of( returnMe, m1 );
                } );

        CollectableTask<SampleData<Pair<Boolean,Boolean>>, MatrixStatistic, DoubleScoreStatistic> task =
                new CollectableTask<>( m,
                                       futureInput );

        //Compute the pairs
        pairPool.submit( futureInput );

        //Should not throw an exception
        DoubleScoreStatistic output = task.call();

        assertEquals( 0.333333, output.getData(), DOUBLE_COMPARE_THRESHOLD );
    }

    @Test
    public void testExceptionOnNullInput() throws ExecutionException, InterruptedException
    {
        final FutureTask<MatrixStatistic> futureInputNull =
                new FutureTask<>( () -> null );

        pairPool.submit( futureInputNull );

        final CollectableTask<SampleData<Pair<Boolean,Boolean>>, MatrixStatistic, DoubleScoreStatistic> task2 =
                new CollectableTask<>( m,
                                       futureInputNull );

        //Should throw an exception
        assertThrows( SampleDataException.class, () -> task2.call() );
    }

    @After
    public void tearDownAfterEachTest()
    {
        pairPool.shutdownNow();
    }
}
